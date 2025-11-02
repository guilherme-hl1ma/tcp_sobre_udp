"""
Implementação do protocolo RDT 2.0 - Protocolo básico com detecção de erros.
Inclui classes RDT20Sender e RDT20Receiver com protocolo stop-and-wait.
"""

import socket
import threading
import time
from typing import Optional, Tuple, Any

from utils.packet import RDT20Packet, Packet, PacketError
from utils.simulator import UnreliableChannel
from utils.logger import ProtocolLogger


class RDT20Sender:
    """
    Remetente RDT 2.0 implementando protocolo stop-and-wait com detecção de erros.
    Aguarda ACK/NAK antes de enviar próximo pacote e retransmite quando recebe NAK.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel, 
                 logger: Optional[ProtocolLogger] = None):
        """
        Inicializa o remetente RDT 2.0.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
        """
        self.socket = socket_obj
        self.channel = channel
        self.logger = logger or ProtocolLogger("RDT20Sender")
        
        # Estados do protocolo
        self.state = "READY"  # READY, WAITING_ACK
        self.last_packet = None
        self.dest_addr = None
        
        # Controle de threading
        self._lock = threading.Lock()
        self._ack_received = threading.Event()
        self._response_packet = None
    
    def send_data(self, data: bytes, dest_addr: Tuple[str, int]) -> bool:
        """
        Envia dados usando protocolo stop-and-wait.
        
        Args:
            data: Dados a serem enviados
            dest_addr: Endereço de destino (host, porta)
            
        Returns:
            bool: True se dados foram enviados com sucesso
        """
        with self._lock:
            if self.state != "READY":
                raise RuntimeError("Sender não está pronto para enviar")
            
            self.dest_addr = dest_addr
            
            # Criar pacote de dados com checksum
            packet = RDT20Packet(Packet.DATA, data)
            self.last_packet = packet
            
            # Loop de transmissão com retransmissão
            max_attempts = 10  # Limite de tentativas
            attempt = 0
            
            while attempt < max_attempts:
                attempt += 1
                
                # Enviar pacote
                self._send_packet(packet)
                
                # Mudar estado para aguardar resposta
                self.state = "WAITING_ACK"
                self._ack_received.clear()
                
                # Aguardar ACK/NAK com timeout
                if self._wait_for_response(timeout=5.0):
                    # Processar resposta recebida
                    if self._process_response():
                        # ACK recebido - transmissão bem-sucedida
                        self.state = "READY"
                        return True
                    else:
                        # NAK recebido - retransmitir
                        self.logger.log_retransmission("NAK recebido", "DATA")
                        continue
                else:
                    # Timeout - retransmitir
                    self.logger.log_retransmission("Timeout", "DATA")
                    continue
            
            # Falha após múltiplas tentativas
            self.state = "READY"
            return False
    
    def _send_packet(self, packet: RDT20Packet):
        """
        Envia pacote através do canal.
        
        Args:
            packet: Pacote a ser enviado
        """
        serialized = packet.serialize()
        
        # Informações para o simulador
        packet_info = {
            'type': packet.get_type_name(),
            'seq_num': None  # RDT 2.0 não usa números de sequência
        }
        
        # Registrar transmissão
        self.logger.log_transmission(
            packet_type=packet.get_type_name(),
            data_size=len(packet.data),
            protocol_overhead=len(serialized) - len(packet.data)
        )
        
        # Enviar através do canal
        self.channel.send(serialized, self.socket, self.dest_addr, packet_info)
    
    def _wait_for_response(self, timeout: float = 5.0) -> bool:
        """
        Aguarda resposta (ACK/NAK) do receptor.
        
        Args:
            timeout: Tempo limite em segundos
            
        Returns:
            bool: True se resposta foi recebida, False se timeout
        """
        # Iniciar thread para receber resposta
        response_thread = threading.Thread(target=self.receive_response)
        response_thread.daemon = True
        response_thread.start()
        
        # Aguardar resposta ou timeout
        return self._ack_received.wait(timeout)
    
    def _process_response(self) -> bool:
        """
        Processa resposta recebida (ACK/NAK).
        
        Returns:
            bool: True se ACK, False se NAK ou erro
        """
        if self._response_packet is None:
            return False
        
        try:
            # Verificar se pacote está corrompido
            if self._response_packet.is_corrupted():
                self.logger.log_corruption("ACK/NAK")
                return False  # Tratar como NAK
            
            # Verificar tipo de resposta
            if self._response_packet.is_ack_packet():
                self.logger.log_reception("ACK", success=True)
                return True
            elif self._response_packet.is_nak_packet():
                self.logger.log_reception("NAK", success=True)
                return False
            else:
                # Tipo inválido
                return False
                
        except Exception as e:
            print(f"Erro ao processar resposta: {e}")
            return False
    
    def handle_ack_nak(self, packet: RDT20Packet):
        """
        Manipula ACK/NAK recebido do receptor.
        
        Args:
            packet: Pacote ACK/NAK recebido
        """
        with self._lock:
            if self.state == "WAITING_ACK":
                self._response_packet = packet
                self._ack_received.set()
    
    def receive_response(self) -> Optional[RDT20Packet]:
        """
        Recebe e processa resposta do socket.
        Método auxiliar para ser chamado em thread separada.
        
        Returns:
            RDT20Packet: Pacote recebido ou None se erro
        """
        try:
            # Configurar timeout no socket
            original_timeout = self.socket.gettimeout()
            self.socket.settimeout(1.0)
            
            while self.state == "WAITING_ACK":
                try:
                    data, addr = self.socket.recvfrom(1024)
                    
                    # Deserializar pacote
                    packet = RDT20Packet.deserialize(data)
                    
                    # Processar apenas se for ACK ou NAK
                    if packet.is_ack_packet() or packet.is_nak_packet():
                        self.handle_ack_nak(packet)
                        return packet
                        
                except socket.timeout:
                    if self.state != "WAITING_ACK":
                        break
                    continue
                except Exception as e:
                    if self.state != "WAITING_ACK":
                        break
                    print(f"Erro ao receber resposta: {e}")
                    continue
            
            return None
            
        except Exception as e:
            print(f"Erro no receive_response: {e}")
            return None
        finally:
            # Restaurar timeout original
            try:
                self.socket.settimeout(original_timeout)
            except:
                pass
    
    def get_state(self) -> str:
        """Retorna estado atual do sender."""
        return self.state
    
    def reset(self):
        """Reseta o sender para estado inicial."""
        with self._lock:
            self.state = "READY"
            self.last_packet = None
            self.dest_addr = None
            self._ack_received.clear()
            self._response_packet = None


class RDT20Receiver:
    """
    Receptor RDT 2.0 implementando verificação de integridade com checksum.
    Envia ACK para pacotes corretos e NAK para pacotes corrompidos.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel,
                 logger: Optional[ProtocolLogger] = None):
        """
        Inicializa o receptor RDT 2.0.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
        """
        self.socket = socket_obj
        self.channel = channel
        self.logger = logger or ProtocolLogger("RDT20Receiver")
        
        # Buffer para dados recebidos
        self.received_data = []
        
        # Controle de threading
        self._lock = threading.Lock()
        self._running = False
    
    def start_receiving(self):
        """Inicia o loop de recepção de dados."""
        self._running = True
        
        while self._running:
            try:
                data_packet = self.receive_data()
                if data_packet and not data_packet.is_corrupted():
                    # Dados válidos recebidos
                    with self._lock:
                        self.received_data.append(data_packet.data)
                        
            except Exception as e:
                print(f"Erro no loop de recepção: {e}")
                break
    
    def receive_data(self) -> Optional[RDT20Packet]:
        """
        Recebe e processa pacotes de dados.
        
        Returns:
            RDT20Packet: Pacote de dados recebido ou None se erro
        """
        try:
            # Configurar timeout no socket
            self.socket.settimeout(1.0)
            
            data, sender_addr = self.socket.recvfrom(1024)
            
            # Deserializar pacote
            packet = RDT20Packet.deserialize(data)
            
            # Registrar recepção
            self.logger.log_reception(
                packet_type=packet.get_type_name(),
                data_size=len(packet.data),
                success=True
            )
            
            # Processar apenas pacotes de dados
            if packet.is_data_packet():
                # Verificar integridade
                if packet.is_corrupted():
                    # Pacote corrompido - enviar NAK
                    self.logger.log_corruption("DATA")
                    self.send_ack_nak(False, sender_addr)
                    return None
                else:
                    # Pacote correto - enviar ACK
                    self.send_ack_nak(True, sender_addr)
                    return packet
            
            return None
            
        except socket.timeout:
            return None
        except PacketError as e:
            print(f"Erro no pacote recebido: {e}")
            return None
        except Exception as e:
            print(f"Erro ao receber dados: {e}")
            return None
    
    def send_ack_nak(self, is_ack: bool, dest_addr: Tuple[str, int]):
        """
        Envia ACK ou NAK para o remetente.
        
        Args:
            is_ack: True para ACK, False para NAK
            dest_addr: Endereço do remetente
        """
        try:
            # Criar pacote de resposta
            packet_type = Packet.ACK if is_ack else Packet.NAK
            response_packet = RDT20Packet(packet_type, b'')
            
            # Serializar e enviar
            serialized = response_packet.serialize()
            
            # Informações para o simulador
            packet_info = {
                'type': response_packet.get_type_name(),
                'seq_num': None
            }
            
            # Registrar transmissão
            self.logger.log_transmission(
                packet_type=response_packet.get_type_name(),
                data_size=0,
                protocol_overhead=len(serialized)
            )
            
            # Enviar através do canal
            self.channel.send(serialized, self.socket, dest_addr, packet_info)
            
        except Exception as e:
            print(f"Erro ao enviar ACK/NAK: {e}")
    
    def get_received_data(self) -> list:
        """
        Retorna dados recebidos até o momento.
        
        Returns:
            list: Lista de dados recebidos
        """
        with self._lock:
            return self.received_data.copy()
    
    def clear_received_data(self):
        """Limpa buffer de dados recebidos."""
        with self._lock:
            self.received_data.clear()
    
    def stop_receiving(self):
        """Para o loop de recepção."""
        self._running = False
    
    def is_running(self) -> bool:
        """Verifica se o receptor está ativo."""
        return self._running
