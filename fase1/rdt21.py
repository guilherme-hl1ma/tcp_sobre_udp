"""
Implementação do protocolo RDT 2.1 - Adição de números de sequência.
Estende RDT 2.0 adicionando números de sequência alternados (0/1) para lidar com ACKs corrompidos.
"""

import socket
import threading
import time
from typing import Optional, Tuple, Any

try:
    from ..utils.logger import ProtocolLogger
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from utils.logger import ProtocolLogger

from utils.packet import RDT21Packet, Packet, PacketError
from utils.simulator import UnreliableChannel
from utils.logger import ProtocolLogger
from fase1.rdt20 import RDT20Sender, RDT20Receiver


class RDT21Sender(RDT20Sender):
    """
    Remetente RDT 2.1 implementando protocolo stop-and-wait com números de sequência.
    Estende RDT20Sender adicionando campo seq_num alternado (0/1) para detectar ACKs duplicados.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel, 
                 logger: Optional[ProtocolLogger] = None):
        """
        Inicializa o remetente RDT 2.1.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
        """
        super().__init__(socket_obj, channel, logger)
        
        # Número de sequência alternado (0 ou 1)
        self.seq_num = 0
        
        # Atualizar logger para RDT21
        if logger is None:
            self.logger = ProtocolLogger("RDT21Sender")
    
    def send_data(self, data: bytes, dest_addr: Tuple[str, int]) -> bool:
        """
        Envia dados usando protocolo stop-and-wait com números de sequência.
        
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
            
            # Criar pacote de dados com número de sequência
            packet = RDT21Packet(Packet.DATA, self.seq_num, data)
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
                
                # Aguardar ACK com timeout
                if self._wait_for_response(timeout=5.0):
                    # Processar resposta recebida
                    if self._process_response():
                        # ACK correto recebido - transmissão bem-sucedida
                        self.state = "READY"
                        # Alternar número de sequência para próximo envio
                        self.seq_num = 1 - self.seq_num
                        return True
                    else:
                        # ACK corrompido ou com número incorreto - retransmitir
                        self.logger.log_retransmission("ACK corrompido/incorreto", "DATA")
                        continue
                else:
                    # Timeout - retransmitir
                    self.logger.log_retransmission("Timeout", "DATA")
                    continue
            
            # Falha após múltiplas tentativas
            self.state = "READY"
            return False
    
    def _send_packet(self, packet: RDT21Packet):
        """
        Envia pacote através do canal.
        
        Args:
            packet: Pacote a ser enviado
        """
        serialized = packet.serialize()
        
        # Informações para o simulador
        packet_info = {
            'type': packet.get_type_name(),
            'seq_num': packet.seq_num
        }
        
        # Registrar transmissão
        self.logger.log_transmission(
            packet_type=packet.get_type_name(),
            seq_num=packet.seq_num,
            data_size=len(packet.data),
            protocol_overhead=len(serialized) - len(packet.data)
        )
        
        # Enviar através do canal
        self.channel.send(serialized, self.socket, self.dest_addr, packet_info)
    
    def _process_response(self) -> bool:
        """
        Processa resposta recebida (ACK) verificando número de sequência.
        
        Returns:
            bool: True se ACK correto, False se ACK corrompido/incorreto
        """
        if self._response_packet is None:
            return False
        
        try:
            # Verificar se pacote está corrompido
            if self._response_packet.is_corrupted():
                self.logger.log_corruption("ACK")
                return False  # Tratar como ACK incorreto
            
            # Verificar se é ACK
            if not self._response_packet.is_ack_packet():
                return False  # Não é ACK
            
            # Verificar número de sequência do ACK
            if hasattr(self._response_packet, 'seq_num'):
                if self._response_packet.seq_num == self.seq_num:
                    # ACK com número de sequência correto
                    self.logger.log_reception("ACK", seq_num=self._response_packet.seq_num, success=True)
                    return True
                else:
                    # ACK com número de sequência incorreto (duplicado)
                    self.logger.log_reception("ACK", seq_num=self._response_packet.seq_num, success=False)
                    return False
            else:
                # ACK sem número de sequência (compatibilidade com RDT 2.0)
                self.logger.log_reception("ACK", seq_num=None, success=True)
                return True
                
        except Exception as e:
            print(f"Erro ao processar resposta: {e}")
            return False
    
    def handle_ack(self, packet: RDT21Packet):
        """
        Manipula ACK recebido verificando número de sequência.
        
        Args:
            packet: Pacote ACK recebido
        """
        with self._lock:
            if self.state == "WAITING_ACK":
                self._response_packet = packet
                self._ack_received.set()
    
    def receive_response(self) -> Optional[RDT21Packet]:
        """
        Recebe e processa resposta do socket.
        Método auxiliar para ser chamado em thread separada.
        
        Returns:
            RDT21Packet: Pacote recebido ou None se erro
        """
        try:
            # Configurar timeout no socket
            self.socket.settimeout(1.0)
            
            while self.state == "WAITING_ACK":
                try:
                    data, addr = self.socket.recvfrom(1024)
                    
                    # Tentar deserializar como RDT21Packet primeiro
                    try:
                        packet = RDT21Packet.deserialize(data)
                    except PacketError:
                        # Se falhar, pode ser um pacote RDT20 (compatibilidade)
                        from utils.packet import RDT20Packet
                        packet = RDT20Packet.deserialize(data)
                        # Converter para RDT21Packet sem seq_num
                        packet = RDT21Packet(packet.type, 0, packet.data, packet.checksum)
                    
                    # Processar apenas se for ACK
                    if packet.is_ack_packet():
                        self.handle_ack(packet)
                        return packet
                        
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"Erro ao receber resposta: {e}")
                    continue
            
            return None
            
        except Exception as e:
            print(f"Erro no receive_response: {e}")
            return None
        finally:
            # Restaurar socket para não-bloqueante
            self.socket.settimeout(None)
    
    def get_seq_num(self) -> int:
        """Retorna número de sequência atual."""
        return self.seq_num
    
    def reset(self):
        """Reseta o sender para estado inicial."""
        super().reset()
        self.seq_num = 0


class RDT21Receiver(RDT20Receiver):
    """
    Receptor RDT 2.1 implementando detecção de duplicatas com números de sequência.
    Estende RDT20Receiver adicionando expected_seq para rastrear sequência esperada.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel,
                 logger: Optional[ProtocolLogger] = None):
        """
        Inicializa o receptor RDT 2.1.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
        """
        super().__init__(socket_obj, channel, logger)
        
        # Número de sequência esperado
        self.expected_seq = 0
        
        # ACK anterior para reenvio em caso de duplicata
        self.last_ack_seq = None
        
        # Atualizar logger para RDT21
        if logger is None:
            self.logger = ProtocolLogger("RDT21Receiver")
    
    def receive_data(self) -> Optional[RDT21Packet]:
        """
        Recebe e processa pacotes de dados verificando número de sequência.
        
        Returns:
            RDT21Packet: Pacote de dados recebido ou None se erro/duplicata
        """
        try:
            # Configurar timeout no socket
            self.socket.settimeout(1.0)
            
            data, sender_addr = self.socket.recvfrom(1024)
            
            # Tentar deserializar como RDT21Packet
            try:
                packet = RDT21Packet.deserialize(data)
            except PacketError:
                # Se falhar, pode ser um pacote RDT20 (compatibilidade)
                from utils.packet import RDT20Packet
                rdt20_packet = RDT20Packet.deserialize(data)
                # Converter para RDT21Packet assumindo seq_num = 0
                packet = RDT21Packet(rdt20_packet.type, 0, rdt20_packet.data, rdt20_packet.checksum)
            
            # Registrar recepção
            self.logger.log_reception(
                packet_type=packet.get_type_name(),
                seq_num=packet.seq_num,
                data_size=len(packet.data),
                success=True
            )
            
            # Processar apenas pacotes de dados
            if packet.is_data_packet():
                # Verificar integridade
                if packet.is_corrupted():
                    # Pacote corrompido - enviar NAK
                    self.logger.log_corruption("DATA")
                    self.send_nak(sender_addr)
                    return None
                
                # Verificar número de sequência
                if packet.seq_num == self.expected_seq:
                    # Pacote esperado - aceitar e enviar ACK
                    self.send_ack(self.expected_seq, sender_addr)
                    
                    # Alternar número de sequência esperado
                    self.expected_seq = 1 - self.expected_seq
                    self.last_ack_seq = packet.seq_num
                    
                    return packet
                else:
                    # Pacote duplicado - descartar e reenviar ACK anterior
                    self.logger.log_reception("DATA", seq_num=packet.seq_num, success=False)
                    
                    # Reenviar ACK do pacote anterior
                    if self.last_ack_seq is not None:
                        self.send_ack(self.last_ack_seq, sender_addr)
                    
                    return None
            
            return None
            
        except socket.timeout:
            return None
        except PacketError as e:
            print(f"Erro no pacote recebido: {e}")
            return None
        except Exception as e:
            print(f"Erro ao receber dados: {e}")
            return None
    
    def send_ack(self, seq_num: int, dest_addr: Tuple[str, int]):
        """
        Envia ACK com número de sequência específico.
        
        Args:
            seq_num: Número de sequência do ACK
            dest_addr: Endereço do remetente
        """
        try:
            # Criar pacote ACK com número de sequência
            ack_packet = RDT21Packet(Packet.ACK, seq_num, b'')
            
            # Serializar e enviar
            serialized = ack_packet.serialize()
            
            # Informações para o simulador
            packet_info = {
                'type': ack_packet.get_type_name(),
                'seq_num': seq_num
            }
            
            # Registrar transmissão
            self.logger.log_transmission(
                packet_type=ack_packet.get_type_name(),
                seq_num=seq_num,
                data_size=0,
                protocol_overhead=len(serialized)
            )
            
            # Enviar através do canal
            self.channel.send(serialized, self.socket, dest_addr, packet_info)
            
        except Exception as e:
            print(f"Erro ao enviar ACK: {e}")
    
    def send_nak(self, dest_addr: Tuple[str, int]):
        """
        Envia NAK para o remetente.
        
        Args:
            dest_addr: Endereço do remetente
        """
        try:
            # Criar pacote NAK (sem número de sequência específico)
            nak_packet = RDT21Packet(Packet.NAK, 0, b'')
            
            # Serializar e enviar
            serialized = nak_packet.serialize()
            
            # Informações para o simulador
            packet_info = {
                'type': nak_packet.get_type_name(),
                'seq_num': 0
            }
            
            # Registrar transmissão
            self.logger.log_transmission(
                packet_type=nak_packet.get_type_name(),
                seq_num=0,
                data_size=0,
                protocol_overhead=len(serialized)
            )
            
            # Enviar através do canal
            self.channel.send(serialized, self.socket, dest_addr, packet_info)
            
        except Exception as e:
            print(f"Erro ao enviar NAK: {e}")
    
    def get_expected_seq(self) -> int:
        """Retorna número de sequência esperado."""
        return self.expected_seq
    
    def reset(self):
        """Reseta o receiver para estado inicial."""
        with self._lock:
            self.expected_seq = 0
            self.last_ack_seq = None
            self.received_data.clear()
