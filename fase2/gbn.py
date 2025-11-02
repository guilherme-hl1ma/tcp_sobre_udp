"""
Implementação do protocolo Go-Back-N (GBN) para transferência confiável de dados.
Implementa pipelining com janela deslizante, confirmações cumulativas e retransmissão de janela completa.
"""

import socket
import threading
import time
from typing import Optional, Tuple, Dict, Any
from utils.packet import PipelinePacket
from utils.simulator import UnreliableChannel, SlidingWindow
from utils.logger import PipelineLogger


class GBNSender:
    """
    Remetente Go-Back-N que gerencia janela deslizante e transmissão de pacotes.
    
    Implementa:
    - Janela deslizante de tamanho N
    - Timer único para o pacote mais antigo não confirmado
    - Processamento de ACKs cumulativos
    - Retransmissão de janela completa em timeout
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel,
                 window_size: int = 5, timeout: float = 2.0,
                 logger: Optional[PipelineLogger] = None):
        """
        Inicializa o remetente GBN.
        
        Args:
            socket_obj: Socket para comunicação
            channel: Canal não confiável para simulação
            window_size: Tamanho da janela deslizante (N)
            timeout: Timeout em segundos para retransmissão
            logger: Logger para métricas (opcional)
        """
        if window_size <= 0:
            raise ValueError(f"Tamanho da janela deve ser positivo: {window_size}")
        if timeout <= 0:
            raise ValueError(f"Timeout deve ser positivo: {timeout}")
        
        self.socket = socket_obj
        self.channel = channel
        self.window_size = window_size
        self.timeout = timeout
        self.logger = logger or PipelineLogger("GBN_Sender")
        
        # Estado da janela deslizante
        self.sliding_window = SlidingWindow(window_size)
        self.base = 0              # Primeiro pacote não confirmado
        self.next_seq_num = 0      # Próximo número de sequência disponível
        
        # Buffer de pacotes para retransmissão
        self.packet_buffer: Dict[int, bytes] = {}
        
        # Gerenciamento de timer
        self.timeout_timer: Optional[threading.Timer] = None
        self.timer_lock = threading.Lock()
        
        # Estado de controle
        self.active = True
        self.window_condition = threading.Condition()
        
        # Thread para recepção de ACKs
        self._ack_thread = None
        self._ack_thread_active = False
        self._dest_addr = None
        
        self._start_ack_receiver()
    
    def can_send(self) -> bool:
        """
        Verifica se pode enviar novo pacote (janela não está cheia).
        
        Returns:
            bool: True se pode enviar, False se janela cheia
        """
        with self.window_condition:
            return self.sliding_window.can_send()
    
    def send_data(self, data: bytes, dest_addr: Tuple[str, int]) -> bool:
        """
        Envia dados usando protocolo GBN.
        Bloqueia se a janela estiver cheia até que espaço esteja disponível.
        
        Args:
            data: Dados a serem enviados
            dest_addr: Endereço de destino (host, porta)
            
        Returns:
            bool: True se enviado com sucesso, False se sender foi desligado
        """
        # Armazenar endereço de destino
        self._dest_addr = dest_addr
        
        with self.window_condition:
            # Espera(bloqueia) até a janela tenha espaço
            while not self.sliding_window.can_send():
                if not self.active:
                    return False
                self.window_condition.wait()

            # Cria pacote com número de sequência de 32 bits
            packet = PipelinePacket(PipelinePacket.DATA, self.next_seq_num, data)
            serialized_packet = packet.serialize()
            
            # Adiciona pacote à janela deslizante antes do envio
            packet_added = self.sliding_window.add_packet(
                self.next_seq_num, 
                len(data), 
                time.time()
            )
            
            if not packet_added:
                return False
            
            # Armazena pacote no buffer para possível retransmissão
            self.packet_buffer[self.next_seq_num] = serialized_packet
            
            # Integra com UnreliableChannel para simulação de perda
            packet_info = {
                'type': 'DATA',
                'seq_num': self.next_seq_num
            }
            
            success = self.channel.send(
                serialized_packet, 
                self.socket, 
                dest_addr, 
                packet_info
            )
            
            # Registra transmissão no PipelineLogger
            if self.logger:
                self.logger.log_transmission(
                    "DATA", 
                    self.next_seq_num, 
                    len(data), 
                    len(serialized_packet) - len(data)  # overhead do protocolo
                )
                
                # Registra estado da janela
                self.logger.log_window_state(
                    self.base,
                    self.next_seq_num + 1,
                    self.window_size,
                    len(self.sliding_window.get_unacked_packets()),
                    self.sliding_window.get_available_slots() - 1
                )
            
            # Inicia timer se for o primeiro pacote (base == next_seq_num)
            if self.next_seq_num == self.base:
                self._start_timeout_timer()
            
            self.next_seq_num += 1
            return success
    
    def _start_timeout_timer(self):
        """Inicia timer para o pacote mais antigo não confirmado (base)."""
        with self.timer_lock:
            # Cancela timer anterior se existir
            if self.timeout_timer is not None:
                self.timeout_timer.cancel()
            
            # Cria novo timer
            self.timeout_timer = threading.Timer(self.timeout, self._handle_timeout)
            self.timeout_timer.start()
    
    def _stop_timeout_timer(self):
        """Para o timer de timeout."""
        with self.timer_lock:
            if self.timeout_timer is not None:
                self.timeout_timer.cancel()
                self.timeout_timer = None
    
    def _restart_timeout_timer(self):
        """Reinicia timer quando janela desliza mas ainda tem pacotes."""
        with self.timer_lock:
            if self.base < self.next_seq_num:  # Ainda há pacotes não confirmados
                self._start_timeout_timer()
            else:
                self._stop_timeout_timer()
    
    def get_window_state(self) -> Dict[str, Any]:
        """
        Retorna estado atual da janela para debugging/monitoramento.
        
        Returns:
            Dict: Estado da janela com informações úteis
        """
        with self.window_condition:
            return {
                'base': self.base,
                'next_seq_num': self.next_seq_num,
                'window_size': self.window_size,
                'packets_in_flight': self.next_seq_num - self.base,
                'can_send': self.can_send(),
                'available_slots': self.sliding_window.get_available_slots(),
                'unacked_packets': len(self.sliding_window.get_unacked_packets()),
                'buffer_size': len(self.packet_buffer)
            }
    
    def reset(self):
        """Reseta o remetente para estado inicial."""
        with self.window_condition:
            self._stop_timeout_timer()
            self.sliding_window.reset()
            self.base = 0
            self.next_seq_num = 0
            self.packet_buffer.clear()
            self.active = True
            
            if self.logger:
                self.logger.start_session()    

    def _handle_timeout(self):
        """
        Detecta expiração do timer de timeout e retransmite janela completa.
        Implementa a lógica de retransmissão de janela completa do Go-Back-N.
        """
        with self.window_condition:
            if not self.active:
                return
            
            # Registra evento de timeout
            if self.logger:
                self.logger.log_timeout(self.base)
            
            # Retransmite todos os pacotes de base até next_seq_num - 1
            self._retransmit_window("timeout")
            
            # Reinicia timer após retransmissão
            self._restart_timeout_timer()
    
    def _retransmit_window(self, reason: str = "timeout", dest_addr: Optional[Tuple[str, int]] = None):
        """
        Retransmite todos os pacotes da janela atual.
        Implementa retransmissão de janela completa conforme protocolo Go-Back-N.
        
        Args:
            reason: Motivo da retransmissão para logging
            dest_addr: Endereço de destino (usa padrão se None)
        """
        if self.base >= self.next_seq_num:
            return  # Nenhum pacote para retransmitir
        
        # Usa endereço padrão se não fornecido
        if dest_addr is None:
            dest_addr = self._get_dest_addr()
        
        # Registra retransmissão de janela no PipelineLogger
        if self.logger:
            self.logger.log_window_retransmission(
                self.base, 
                self.next_seq_num - 1, 
                reason
            )
        
        # Retransmite todos os pacotes de base até next_seq_num - 1
        retransmitted_count = 0
        for seq_num in range(self.base, self.next_seq_num):
            if seq_num in self.packet_buffer:
                # Incrementa contador de retransmissões no sliding window
                self.sliding_window.increment_retransmissions(seq_num)
                
                # Reenvia o pacote
                packet_info = {
                    'type': 'DATA_RETRANSMIT',
                    'seq_num': seq_num
                }
                
                success = self.channel.send(
                    self.packet_buffer[seq_num],
                    self.socket,
                    dest_addr,
                    packet_info
                )
                
                if success:
                    retransmitted_count += 1
                
                # Registra retransmissão individual no logger
                if self.logger:
                    self.logger.log_retransmission(reason, "DATA", seq_num)
        
        return retransmitted_count
    
    def _get_dest_addr(self) -> Tuple[str, int]:
        """
        Retorna endereço de destino armazenado.
        """
        return self._dest_addr or ("localhost", 12345)
    
    def _start_ack_receiver(self):
        """Inicia thread para recepção de ACKs."""
        if not self._ack_thread_active:
            self._ack_thread_active = True
            self._ack_thread = threading.Thread(target=self._ack_receiver_loop, daemon=True)
            self._ack_thread.start()
    
    def _ack_receiver_loop(self):
        """Loop principal para recepção de ACKs."""
        while self._ack_thread_active and self.active:
            try:
                # Configurar timeout mais longo para reduzir CPU usage
                self.socket.settimeout(0.5)
                raw_data, sender_addr = self.socket.recvfrom(1024)
                
                # Processar ACK recebido
                self.receive_ack(raw_data)
                
            except socket.timeout:
                # Timeout é normal - continua o loop
                continue
            except Exception as e:
                # Erro na recepção - aguarda mais tempo antes de tentar novamente
                if self.active:
                    time.sleep(0.1)
                continue
    
    def receive_ack(self, ack_data: bytes) -> bool:
        """
        Processa ACKs cumulativos recebidos.
        
        Args:
            ack_data: Dados do pacote ACK recebido
            
        Returns:
            bool: True se ACK foi processado, False se inválido/corrompido
        """
        try:
            # Deserializa pacote ACK
            ack_packet = PipelinePacket.deserialize(ack_data)
            
            # Valida integridade de pacotes ACK recebidos
            if ack_packet.is_corrupted():
                if self.logger:
                    self.logger.log_corruption("ACK", ack_packet.seq_num)
                return False
            
            # Verifica se é realmente um ACK
            if not ack_packet.is_ack_packet():
                return False
            
            with self.window_condition:
                # Ignora ACKs duplicados ou fora de ordem
                if ack_packet.seq_num < self.base:
                    # ACK duplicado - ignora
                    return True
                
                if ack_packet.seq_num >= self.next_seq_num:
                    # ACK para pacote não enviado ainda - ignora
                    return True
                
                # Processa ACKs cumulativos movendo base para seq_num + 1
                old_base = self.base
                self.base = ack_packet.seq_num + 1
                
                # Desliza janela removendo pacotes confirmados
                packets_acked = self._slide_window(old_base, self.base)
                
                # Registra eventos de deslizamento no logger
                if self.logger and packets_acked > 0:
                    self.logger.log_window_slide(old_base, self.base, packets_acked)
                    self.logger.log_reception("ACK", ack_packet.seq_num, 0, True)
                
                # Gerencia timer baseado no estado da janela
                if self.base == self.next_seq_num:
                    # Para timer quando janela fica vazia (base == next_seq_num)
                    self._stop_timeout_timer()
                else:
                    # Reinicia timer quando janela desliza mas ainda tem pacotes
                    self._restart_timeout_timer()
                
                # Notifica a thread send_data que a janela pode ter espaço
                self.window_condition.notify_all()
                
                return True
                
        except Exception as e:
            # ACK malformado
            if self.logger:
                self.logger.log_corruption("ACK", None)
            return False
    
    def _slide_window(self, old_base: int, new_base: int) -> int:
        """
        Desliza janela removendo pacotes confirmados.
        
        Args:
            old_base: Base anterior da janela
            new_base: Nova base da janela
            
        Returns:
            int: Número de pacotes confirmados
        """
        packets_acked = 0
        
        # Remove pacotes confirmados do buffer e marca como confirmados
        for seq_num in range(old_base, new_base):
            if seq_num in self.packet_buffer:
                del self.packet_buffer[seq_num]
                packets_acked += 1
            
            # Marca pacote como confirmado no sliding window
            self.sliding_window.ack_packet(seq_num)
        
        # Desliza a janela no sliding window
        slides = self.sliding_window.slide_window()
        
        return packets_acked
    
    def force_retransmit_window(self, dest_addr: Tuple[str, int], reason: str = "manual") -> int:
        """
        Força retransmissão de janela completa (para testes ou recuperação manual).
        
        Args:
            dest_addr: Endereço de destino
            reason: Motivo da retransmissão
            
        Returns:
            int: Número de pacotes retransmitidos
        """
        with self.window_condition:
            if not self.active:
                return 0
            
            return self._retransmit_window(reason, dest_addr)
    
    def get_timeout_info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o estado do timer de timeout.
        
        Returns:
            Dict: Informações do timer
        """
        with self.timer_lock:
            return {
                'timer_active': self.timeout_timer is not None,
                'timeout_duration': self.timeout,
                'oldest_unacked': self.sliding_window.get_oldest_unacked(),
                'packets_awaiting_ack': self.next_seq_num - self.base
            }
    
    def set_timeout(self, new_timeout: float):
        """
        Atualiza valor de timeout.
        
        Args:
            new_timeout: Novo valor de timeout em segundos
        """
        if new_timeout <= 0:
            raise ValueError(f"Timeout deve ser positivo: {new_timeout}")
        
        with self.window_condition:
            self.timeout = new_timeout
            # Reinicia timer com novo valor se estiver ativo
            if self.base < self.next_seq_num:
                self._restart_timeout_timer()
    
    def try_receive_ack(self, timeout: float = 0.1) -> bool:
        """
        Tenta receber um ACK sem bloquear por muito tempo.
        
        Args:
            timeout: Timeout para recepção
            
        Returns:
            bool: True se ACK foi recebido e processado
        """
        try:
            original_timeout = self.socket.gettimeout()
            self.socket.settimeout(timeout)
            raw_data, sender_addr = self.socket.recvfrom(1024)
            self.socket.settimeout(original_timeout)
            
            return self.receive_ack(raw_data)
        except socket.timeout:
            return False
        except Exception:
            return False
    
    def shutdown(self):
        """Encerra o remetente e limpa recursos."""
        with self.window_condition:
            self.active = False
            self._ack_thread_active = False
            self._stop_timeout_timer()
            
            # Aguardar thread de ACK terminar
            if self._ack_thread and self._ack_thread.is_alive():
                self._ack_thread.join(timeout=1.0)


class GBNReceiver:
    """
    Receptor Go-Back-N que processa pacotes sequencialmente.
    
    Implementa:
    - Recepção e validação de pacotes de dados
    - Entrega sequencial de dados à aplicação
    - Envio de ACKs cumulativos
    - Descarte de pacotes fora de ordem
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel,
                 logger: Optional[PipelineLogger] = None):
        """
        Inicializa o receptor GBN com expected_seq_num = 0.
        
        Args:
            socket_obj: Socket para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para métricas (opcional)
        """
        self.socket = socket_obj
        self.channel = channel
        self.logger = logger or PipelineLogger("GBN_Receiver")
        
        # Estado de recepção sequencial
        self.expected_seq_num = 0  # Próximo número de sequência esperado
        self.last_ack_sent = -1    # Último ACK enviado para reenvio em duplicatas
        
        # Estado de controle
        self.active = True
        self._lock = threading.Lock()
    
    def receive_data(self, timeout: float = 5.0) -> Optional[bytes]:
        """
        Processa pacotes recebidos com validação e entrega sequencial.
        
        Args:
            timeout: Timeout para recepção em segundos
            
        Returns:
            Optional[bytes]: Dados recebidos se pacote correto, None caso contrário
        """
        try:
            # Configura timeout no socket
            self.socket.settimeout(timeout)
            
            # Recebe dados do socket
            raw_data, sender_addr = self.socket.recvfrom(4096)
            
            # Deserializa pacotes recebidos usando PipelinePacket
            try:
                packet = PipelinePacket.deserialize(raw_data)
            except Exception as e:
                # Pacote malformado - registra e ignora
                if self.logger:
                    self.logger.log_corruption("MALFORMED", None)
                return None
            
            # Registra eventos de recepção no logger
            if self.logger:
                self.logger.log_reception(
                    packet.get_type_name(),
                    packet.seq_num,
                    len(packet.data),
                    True  # Recepção bem-sucedida (ainda não validada)
                )
            
            # Valida integridade usando checksum do pacote
            if packet.is_corrupted():
                # Pacote corrompido - registra e reenvia último ACK
                if self.logger:
                    self.logger.log_corruption(packet.get_type_name(), packet.seq_num)
                
                self._send_last_ack(sender_addr)
                return None
            
            # Verifica se é pacote de dados
            if not packet.is_data_packet():
                # Não é pacote de dados - ignora
                return None
            
            # Verifica se número de sequência é o esperado
            with self.window_condition:
                if packet.seq_num == self.expected_seq_num:
                    # Pacote correto recebido - processa
                    return self._process_correct_packet(packet, sender_addr)
                else:
                    # Pacote fora de ordem - descarta e reenvia último ACK
                    return self._process_out_of_order_packet(packet, sender_addr)
        
        except socket.timeout:
            # Timeout na recepção - retorna None
            return None
        except Exception as e:
            # Erro na recepção - registra e retorna None
            if self.logger:
                self.logger.log_corruption("RECEIVE_ERROR", None)
            return None
    
    def _process_correct_packet(self, packet: PipelinePacket, sender_addr: Tuple[str, int]) -> bytes:
        """
        Processa pacote com número de sequência correto.
        
        Args:
            packet: Pacote recebido corretamente
            sender_addr: Endereço do remetente
            
        Returns:
            bytes: Dados do pacote para entrega à aplicação
        """
        # Envia ACK cumulativo para o pacote recebido
        self._send_ack(packet.seq_num, sender_addr)
        
        # Incrementa expected_seq_num após entrega bem-sucedida
        self.expected_seq_num += 1
        
        # Registra entrega sequencial no logger
        if self.logger:
            self.logger.log_reception(
                "DATA_DELIVERED",
                packet.seq_num,
                len(packet.data),
                True
            )
        
        # Retorna dados para entrega à aplicação
        return packet.data
    
    def _process_out_of_order_packet(self, packet: PipelinePacket, sender_addr: Tuple[str, int]) -> None:
        """
        Processa pacote fora de ordem (descarta sem bufferização).
        
        Args:
            packet: Pacote fora de ordem
            sender_addr: Endereço do remetente
        """
        # Registra pacote fora de ordem no logger
        if self.logger:
            self.logger.log_out_of_order_packet(
                self.expected_seq_num,
                packet.seq_num,
                "discard"
            )
        
        # Reenvia último ACK válido para pacotes fora de ordem
        self._send_last_ack(sender_addr)
        
        return None
    
    def _send_ack(self, seq_num: int, dest_addr: Tuple[str, int]):
        """
        Envia ACK cumulativo com seq_num do pacote recebido corretamente.
        
        Args:
            seq_num: Número de sequência do pacote confirmado
            dest_addr: Endereço de destino
        """
        try:
            # Usa PipelinePacket para formato de ACK
            ack_packet = PipelinePacket(PipelinePacket.ACK, seq_num, b'')
            serialized_ack = ack_packet.serialize()
            
            # Integra com UnreliableChannel para envio
            packet_info = {
                'type': 'ACK',
                'seq_num': seq_num
            }
            
            success = self.channel.send(
                serialized_ack,
                self.socket,
                dest_addr,
                packet_info
            )
            
            if success:
                # Atualiza último ACK enviado
                with self.window_condition:
                    self.last_ack_sent = seq_num
                
                # Registra envio de ACK no logger
                if self.logger:
                    self.logger.log_transmission(
                        "ACK",
                        seq_num,
                        0,  # ACK não tem dados úteis
                        len(serialized_ack)  # overhead do protocolo
                    )
        
        except Exception as e:
            # Erro no envio de ACK - registra mas não interrompe
            if self.logger:
                self.logger.log_corruption("ACK_SEND_ERROR", seq_num)
    
    def _send_last_ack(self, dest_addr: Tuple[str, int]):
        """
        Reenvia último ACK válido para pacotes fora de ordem.
        
        Args:
            dest_addr: Endereço de destino
        """
        with self.window_condition:
            if self.last_ack_sent >= 0:
                # Reenvia último ACK válido
                self._send_ack(self.last_ack_sent, dest_addr)
            else:
                # Ainda não enviou nenhum ACK - não faz nada
                pass
    
    def get_expected_seq(self) -> int:
        """
        Retorna o número de sequência esperado.
        
        Returns:
            int: Próximo número de sequência esperado
        """
        with self.window_condition:
            return self.expected_seq_num
    
    def get_receiver_state(self) -> Dict[str, Any]:
        """
        Retorna estado atual do receptor para debugging/monitoramento.
        
        Returns:
            Dict: Estado do receptor com informações úteis
        """
        with self.window_condition:
            return {
                'expected_seq_num': self.expected_seq_num,
                'last_ack_sent': self.last_ack_sent,
                'active': self.active
            }
    
    def reset(self):
        """Reseta o receptor para estado inicial."""
        with self.window_condition:
            self.expected_seq_num = 0
            self.last_ack_sent = -1
            self.active = True
            
            if self.logger:
                self.logger.start_session()