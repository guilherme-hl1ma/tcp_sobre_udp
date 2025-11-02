"""
Simulador de canal de comunicação não confiável para testes de protocolos RDT.
Implementa perda, corrupção e atraso de pacotes com integração ao sistema de logging.
"""

import random
import threading
import time
from typing import Optional, Tuple, Callable, Any


class UnreliableChannel:
    """
    Simula um canal de comunicação não confiável com perda, corrupção e atraso.
    Integra com sistema de logging para coleta de métricas durante testes.
    """
    
    def __init__(self, loss_rate: float = 0.1, corrupt_rate: float = 0.1, 
                 delay_range: Tuple[float, float] = (0.01, 0.5), 
                 logger: Optional[Any] = None, verbose: bool = True):
        """
        Inicializa o simulador de canal não confiável.
        
        Args:
            loss_rate: Probabilidade de perda de pacote (0.0 a 1.0)
            corrupt_rate: Probabilidade de corrupção (0.0 a 1.0)
            delay_range: Tupla (min_delay, max_delay) em segundos
            logger: Instância do ProtocolLogger para registrar eventos
            verbose: Se deve imprimir mensagens de debug
        """
        if not (0.0 <= loss_rate <= 1.0):
            raise ValueError("loss_rate deve estar entre 0.0 e 1.0")
        if not (0.0 <= corrupt_rate <= 1.0):
            raise ValueError("corrupt_rate deve estar entre 0.0 e 1.0")
        if delay_range[0] < 0 or delay_range[1] < delay_range[0]:
            raise ValueError("delay_range inválido")
        
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range
        self.logger = logger
        self.verbose = verbose
        
        # Estatísticas do simulador
        self.packets_processed = 0
        self.packets_lost = 0
        self.packets_corrupted = 0
        self.packets_delivered = 0
    
    def send(self, packet: bytes, dest_socket: Any, dest_addr: Tuple[str, int], 
             packet_info: Optional[dict] = None) -> bool:
        """
        Envia pacote através do canal não confiável simulando condições reais.
        
        Args:
            packet: Dados do pacote a ser enviado
            dest_socket: Socket de destino
            dest_addr: Endereço de destino (host, porta)
            packet_info: Informações adicionais do pacote para logging
            
        Returns:
            bool: True se pacote foi enviado (pode estar corrompido), False se perdido
        """
        self.packets_processed += 1
        
        # Extrair informações do pacote para logging
        packet_type = packet_info.get('type', 'UNKNOWN') if packet_info else 'UNKNOWN'
        seq_num = packet_info.get('seq_num') if packet_info else None
        
        # Simular perda de pacote
        if random.random() < self.loss_rate:
            self.packets_lost += 1
            if self.verbose:
                print(f"[SIMULADOR] Pacote {packet_type} perdido (seq={seq_num})")
            
            # Registrar perda no logger
            if self.logger:
                self.logger.log_loss(packet_type, seq_num)
            
            return False
        
        # Simular corrupção de pacote
        corrupted = False
        if random.random() < self.corrupt_rate:
            packet = self._corrupt_packet(packet)
            corrupted = True
            self.packets_corrupted += 1
            
            if self.verbose:
                print(f"[SIMULADOR] Pacote {packet_type} corrompido (seq={seq_num})")
            
            # Registrar corrupção no logger
            if self.logger:
                self.logger.log_corruption(packet_type, seq_num)
        
        # Simular atraso e enviar
        delay = random.uniform(*self.delay_range)
        
        def delayed_send():
            """Função para envio com atraso."""
            try:
                dest_socket.sendto(packet, dest_addr)
                self.packets_delivered += 1
                
                if self.verbose and not corrupted:
                    print(f"[SIMULADOR] Pacote {packet_type} entregue após {delay:.3f}s (seq={seq_num})")
                
            except Exception as e:
                if self.verbose:
                    print(f"[SIMULADOR] Erro no envio: {e}")
        
        # Usar timer para simular atraso
        timer = threading.Timer(delay, delayed_send)
        timer.start()
        
        return True
    
    def _corrupt_packet(self, packet: bytes) -> bytes:
        """
        Corrompe bits aleatórios do pacote.
        
        Args:
            packet: Pacote original
            
        Returns:
            bytes: Pacote corrompido
        """
        if len(packet) == 0:
            return packet
        
        packet_list = list(packet)
        
        # Número de corrupções baseado no tamanho do pacote
        max_corruptions = min(5, len(packet))
        num_corruptions = random.randint(1, max_corruptions)
        
        # Corromper bytes aleatórios
        corrupted_positions = set()
        for _ in range(num_corruptions):
            idx = random.randint(0, len(packet_list) - 1)
            if idx not in corrupted_positions:
                # Inverter alguns bits aleatórios no byte
                corruption_mask = random.randint(1, 255)
                packet_list[idx] = packet_list[idx] ^ corruption_mask
                corrupted_positions.add(idx)
        
        return bytes(packet_list)
    
    def get_statistics(self) -> dict:
        """
        Retorna estatísticas do simulador.
        
        Returns:
            dict: Estatísticas de operação do canal
        """
        return {
            'packets_processed': self.packets_processed,
            'packets_lost': self.packets_lost,
            'packets_corrupted': self.packets_corrupted,
            'packets_delivered': self.packets_delivered,
            'loss_rate_actual': self.packets_lost / self.packets_processed if self.packets_processed > 0 else 0,
            'corruption_rate_actual': self.packets_corrupted / self.packets_processed if self.packets_processed > 0 else 0,
            'delivery_rate': self.packets_delivered / self.packets_processed if self.packets_processed > 0 else 0
        }
    
    def reset_statistics(self):
        """Reseta as estatísticas do simulador."""
        self.packets_processed = 0
        self.packets_lost = 0
        self.packets_corrupted = 0
        self.packets_delivered = 0
    
    def set_conditions(self, loss_rate: Optional[float] = None, 
                      corrupt_rate: Optional[float] = None,
                      delay_range: Optional[Tuple[float, float]] = None):
        """
        Atualiza condições do canal durante execução.
        
        Args:
            loss_rate: Nova taxa de perda (opcional)
            corrupt_rate: Nova taxa de corrupção (opcional)
            delay_range: Novo intervalo de atraso (opcional)
        """
        if loss_rate is not None:
            if not (0.0 <= loss_rate <= 1.0):
                raise ValueError("loss_rate deve estar entre 0.0 e 1.0")
            self.loss_rate = loss_rate
        
        if corrupt_rate is not None:
            if not (0.0 <= corrupt_rate <= 1.0):
                raise ValueError("corrupt_rate deve estar entre 0.0 e 1.0")
            self.corrupt_rate = corrupt_rate
        
        if delay_range is not None:
            if delay_range[0] < 0 or delay_range[1] < delay_range[0]:
                raise ValueError("delay_range inválido")
            self.delay_range = delay_range
    
    def __str__(self) -> str:
        """Representação string do simulador."""
        return (f"UnreliableChannel(loss={self.loss_rate:.1%}, "
                f"corrupt={self.corrupt_rate:.1%}, "
                f"delay={self.delay_range[0]:.3f}-{self.delay_range[1]:.3f}s)")


class PerfectChannel(UnreliableChannel):
    """
    Canal perfeito sem perda, corrupção ou atraso para testes de baseline.
    """
    
    def __init__(self, logger: Optional[Any] = None, verbose: bool = True):
        """
        Inicializa canal perfeito.
        
        Args:
            logger: Instância do ProtocolLogger
            verbose: Se deve imprimir mensagens de debug
        """
        super().__init__(loss_rate=0.0, corrupt_rate=0.0, 
                        delay_range=(0.0, 0.0), logger=logger, verbose=verbose)
    
    def send(self, packet: bytes, dest_socket: Any, dest_addr: Tuple[str, int], 
             packet_info: Optional[dict] = None) -> bool:
        """
        Envia pacote imediatamente sem alterações.
        
        Args:
            packet: Dados do pacote
            dest_socket: Socket de destino
            dest_addr: Endereço de destino
            packet_info: Informações do pacote
            
        Returns:
            bool: Sempre True (canal perfeito)
        """
        self.packets_processed += 1
        
        try:
            dest_socket.sendto(packet, dest_addr)
            self.packets_delivered += 1
            
            if self.verbose and packet_info:
                packet_type = packet_info.get('type', 'UNKNOWN')
                seq_num = packet_info.get('seq_num')
                print(f"[CANAL PERFEITO] Pacote {packet_type} entregue (seq={seq_num})")
            
            return True
            
        except Exception as e:
            if self.verbose:
                print(f"[CANAL PERFEITO] Erro no envio: {e}")
            return False


# ===== CLASSES DE JANELA DESLIZANTE PARA PIPELINING =====

from dataclasses import dataclass


@dataclass
class PacketInfo:
    """Informações sobre um pacote na janela."""
    seq_num: int
    timestamp: float
    data_size: int = 0
    acknowledged: bool = False
    retransmissions: int = 0
    
    def __post_init__(self):
        """Garante que timestamp seja definido se não fornecido."""
        if self.timestamp == 0:
            self.timestamp = time.time()


class SlidingWindow:
    """
    Gerenciador de janela deslizante para protocolos de pipelining.
    
    Mantém controle sobre pacotes em trânsito, capacidade da janela e 
    deslizamento baseado em confirmações recebidas.
    """
    
    def __init__(self, window_size: int):
        """
        Inicializa a janela deslizante.
        
        Args:
            window_size: Tamanho máximo da janela (N)
            
        Raises:
            ValueError: Se window_size for inválido
        """
        if window_size <= 0:
            raise ValueError(f"Tamanho da janela deve ser positivo: {window_size}")
        
        self.window_size = window_size
        self.base = 0              # Primeiro pacote não confirmado
        self.next_seq_num = 0      # Próximo número de sequência disponível
        self.window: dict[int, PacketInfo] = {}  # Pacotes na janela {seq_num: PacketInfo}
        self._lock = threading.Lock()  # Thread safety
    
    def can_send(self) -> bool:
        """
        Verifica se pode enviar novo pacote (janela não está cheia).
        
        Returns:
            bool: True se pode enviar, False se janela cheia
        """
        with self._lock:
            return self.next_seq_num < self.base + self.window_size
    
    def get_available_slots(self) -> int:
        """
        Retorna número de slots disponíveis na janela.
        
        Returns:
            int: Número de pacotes que ainda podem ser enviados
        """
        with self._lock:
            used_slots = self.next_seq_num - self.base
            return max(0, self.window_size - used_slots)
    
    def add_packet(self, seq_num: int, data_size: int = 0, 
                   timestamp: Optional[float] = None) -> bool:
        """
        Adiciona pacote à janela se houver espaço.
        
        Args:
            seq_num: Número de sequência do pacote
            data_size: Tamanho dos dados do pacote
            timestamp: Timestamp de envio (usa time.time() se None)
            
        Returns:
            bool: True se adicionado com sucesso, False se janela cheia
        """
        with self._lock:
            # Verifica se pode adicionar
            if seq_num != self.next_seq_num:
                return False  # Deve ser sequencial
            
            if not self.can_send():
                return False  # Janela cheia
            
            # Adiciona pacote
            packet_info = PacketInfo(
                seq_num=seq_num,
                timestamp=timestamp or time.time(),
                data_size=data_size
            )
            self.window[seq_num] = packet_info
            self.next_seq_num += 1
            
            return True
    
    def ack_packet(self, seq_num: int) -> bool:
        """
        Marca pacote como confirmado.
        
        Args:
            seq_num: Número de sequência do pacote confirmado
            
        Returns:
            bool: True se pacote estava na janela, False caso contrário
        """
        with self._lock:
            if seq_num in self.window:
                self.window[seq_num].acknowledged = True
                return True
            return False
    
    def is_packet_acked(self, seq_num: int) -> bool:
        """
        Verifica se pacote foi confirmado.
        
        Args:
            seq_num: Número de sequência a verificar
            
        Returns:
            bool: True se confirmado, False caso contrário
        """
        with self._lock:
            return seq_num in self.window and self.window[seq_num].acknowledged
    
    def get_unacked_packets(self) -> list[int]:
        """
        Retorna lista de pacotes não confirmados na janela.
        
        Returns:
            List[int]: Lista ordenada de números de sequência não confirmados
        """
        with self._lock:
            unacked = []
            for seq_num in range(self.base, self.next_seq_num):
                if seq_num in self.window and not self.window[seq_num].acknowledged:
                    unacked.append(seq_num)
            return sorted(unacked)
    
    def get_oldest_unacked(self) -> Optional[int]:
        """
        Retorna o número de sequência do pacote mais antigo não confirmado.
        
        Returns:
            Optional[int]: Seq_num do mais antigo ou None se todos confirmados
        """
        with self._lock:
            for seq_num in range(self.base, self.next_seq_num):
                if seq_num in self.window and not self.window[seq_num].acknowledged:
                    return seq_num
            return None
    
    def slide_window(self) -> int:
        """
        Desliza janela removendo pacotes confirmados consecutivos da base.
        
        Returns:
            int: Número de posições que a janela deslizou
        """
        with self._lock:
            old_base = self.base
            
            # Desliza enquanto base estiver confirmado
            while (self.base < self.next_seq_num and 
                   self.base in self.window and 
                   self.window[self.base].acknowledged):
                
                # Remove pacote confirmado da janela
                del self.window[self.base]
                self.base += 1
            
            return self.base - old_base
    
    def get_packet_info(self, seq_num: int) -> Optional[PacketInfo]:
        """
        Retorna informações sobre um pacote na janela.
        
        Args:
            seq_num: Número de sequência do pacote
            
        Returns:
            Optional[PacketInfo]: Informações do pacote ou None se não encontrado
        """
        with self._lock:
            return self.window.get(seq_num)
    
    def increment_retransmissions(self, seq_num: int) -> bool:
        """
        Incrementa contador de retransmissões de um pacote.
        
        Args:
            seq_num: Número de sequência do pacote
            
        Returns:
            bool: True se pacote encontrado, False caso contrário
        """
        with self._lock:
            if seq_num in self.window:
                self.window[seq_num].retransmissions += 1
                return True
            return False
    
    def get_window_state(self) -> dict[str, Any]:
        """
        Retorna estado atual da janela para debugging/logging.
        
        Returns:
            Dict: Estado da janela com métricas úteis
        """
        with self._lock:
            unacked_count = len(self.get_unacked_packets())
            
            return {
                'base': self.base,
                'next_seq_num': self.next_seq_num,
                'window_size': self.window_size,
                'packets_in_window': len(self.window),
                'unacked_packets': unacked_count,
                'available_slots': self.get_available_slots(),
                'window_utilization': len(self.window) / self.window_size,
                'oldest_unacked': self.get_oldest_unacked()
            }
    
    def reset(self):
        """Reseta a janela para estado inicial."""
        with self._lock:
            self.base = 0
            self.next_seq_num = 0
            self.window.clear()


class ReceiveWindow:
    """
    Janela de recepção para protocolo Selective Repeat.
    
    Gerencia buffer para pacotes fora de ordem e entrega sequencial à aplicação.
    """
    
    def __init__(self, window_size: int):
        """
        Inicializa a janela de recepção.
        
        Args:
            window_size: Tamanho da janela de recepção
            
        Raises:
            ValueError: Se window_size for inválido
        """
        if window_size <= 0:
            raise ValueError(f"Tamanho da janela deve ser positivo: {window_size}")
        
        self.window_size = window_size
        self.base = 0              # Primeiro pacote esperado
        self.buffer: dict[int, bytes] = {}  # Buffer para pacotes fora de ordem
        self._lock = threading.Lock()
    
    def is_in_window(self, seq_num: int) -> bool:
        """
        Verifica se número de sequência está na janela de recepção.
        
        Args:
            seq_num: Número de sequência a verificar
            
        Returns:
            bool: True se está na janela, False caso contrário
        """
        with self._lock:
            return self.base <= seq_num < self.base + self.window_size
    
    def is_already_received(self, seq_num: int) -> bool:
        """
        Verifica se pacote já foi recebido (está no buffer).
        
        Args:
            seq_num: Número de sequência a verificar
            
        Returns:
            bool: True se já recebido, False caso contrário
        """
        with self._lock:
            return seq_num in self.buffer
    
    def buffer_packet(self, seq_num: int, data: bytes) -> bool:
        """
        Armazena pacote no buffer se estiver na janela e não for duplicata.
        
        Args:
            seq_num: Número de sequência do pacote
            data: Dados do pacote
            
        Returns:
            bool: True se armazenado, False se fora da janela ou duplicata
        """
        with self._lock:
            # Verifica se está na janela
            if not self.is_in_window(seq_num):
                return False
            
            # Verifica se não é duplicata
            if seq_num in self.buffer:
                return False  # Já recebido
            
            # Armazena no buffer
            self.buffer[seq_num] = data
            return True
    
    def deliver_packets(self) -> list[bytes]:
        """
        Entrega pacotes consecutivos a partir da base e desliza janela.
        
        Returns:
            List[bytes]: Lista de dados dos pacotes entregues em ordem
        """
        with self._lock:
            delivered = []
            
            # Entrega pacotes consecutivos a partir da base
            while self.base in self.buffer:
                delivered.append(self.buffer[self.base])
                del self.buffer[self.base]
                self.base += 1
            
            return delivered
    
    def get_expected_seq_num(self) -> int:
        """
        Retorna o próximo número de sequência esperado (base da janela).
        
        Returns:
            int: Número de sequência esperado
        """
        with self._lock:
            return self.base
    
    def get_buffered_packets(self) -> list[int]:
        """
        Retorna lista de pacotes atualmente no buffer.
        
        Returns:
            List[int]: Lista ordenada de números de sequência no buffer
        """
        with self._lock:
            return sorted(self.buffer.keys())
    
    def get_window_range(self) -> tuple[int, int]:
        """
        Retorna o range atual da janela de recepção.
        
        Returns:
            tuple: (início, fim) da janela [início, fim)
        """
        with self._lock:
            return (self.base, self.base + self.window_size)
    
    def get_receive_state(self) -> dict[str, Any]:
        """
        Retorna estado atual da janela de recepção.
        
        Returns:
            Dict: Estado da janela com informações úteis
        """
        with self._lock:
            window_start, window_end = self.get_window_range()
            
            return {
                'base': self.base,
                'window_size': self.window_size,
                'window_range': f"[{window_start}, {window_end})",
                'buffered_packets': len(self.buffer),
                'buffered_seq_nums': self.get_buffered_packets(),
                'next_expected': self.base,
                'buffer_utilization': len(self.buffer) / self.window_size
            }
    
    def reset(self):
        """Reseta a janela de recepção para estado inicial."""
        with self._lock:
            self.base = 0
            self.buffer.clear()