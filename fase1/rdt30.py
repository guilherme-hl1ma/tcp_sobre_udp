"""
Implementação do protocolo RDT 3.0 - Adição de timer e tratamento de perda.
Estende RDT 2.1 adicionando timer de timeout para detectar perda de pacotes e ACKs.
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
from fase1.rdt21 import RDT21Sender, RDT21Receiver


class RDT30Sender(RDT21Sender):
    """
    Remetente RDT 3.0 implementando protocolo stop-and-wait com timer de timeout.
    Estende RDT21Sender adicionando timer para detectar perda de pacotes e ACKs.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel, 
                 logger: Optional[ProtocolLogger] = None, timeout: float = 2.0):
        """
        Inicializa o remetente RDT 3.0.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
            timeout: Tempo limite em segundos para timeout (padrão: 2.0s)
        """
        super().__init__(socket_obj, channel, logger)
        
        # Configuração do timer
        self.timeout = timeout
        self.timer = None
        
        # Atualizar logger para RDT30
        if logger is None:
            self.logger = ProtocolLogger("RDT30Sender")
    
    def send_data(self, data: bytes, dest_addr: Tuple[str, int]) -> bool:
        """
        Envia dados usando protocolo stop-and-wait com timer de timeout.
        
        Args:
            data: Dados a serem enviados
            dest_addr: Endereço de destino (host, porta)
            
        Returns:
            bool: True se dados foram enviados com sucesso
        """
        if self.state != "READY":
            raise RuntimeError("Sender não está pronto para enviar")
        
        self.dest_addr = dest_addr
        
        # Criar pacote de dados com número de sequência
        packet = RDT21Packet(Packet.DATA, self.seq_num, data)
        self.last_packet = packet
        
        # Loop de transmissão com retransmissão
        max_attempts = 5  # Reduzir tentativas
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            
            # Enviar pacote
            self._send_packet(packet)
            
            # Aguardar ACK com timeout
            if self._wait_for_ack_with_timeout():
                # ACK correto recebido - transmissão bem-sucedida
                self.seq_num = 1 - self.seq_num  # Alternar número de sequência
                return True
            else:
                # Timeout ou ACK incorreto - retransmitir
                self.logger.log_retransmission("Timeout ou ACK incorreto", "DATA")
                continue
        
        # Falha após múltiplas tentativas
        return False
    
    def _wait_for_ack_with_timeout(self) -> bool:
        """
        Aguarda ACK com timeout usando socket timeout simples.
        
        Returns:
            bool: True se ACK correto recebido, False se timeout ou ACK incorreto
        """
        try:
            # Configurar timeout no socket
            original_timeout = self.socket.gettimeout()
            self.socket.settimeout(self.timeout)
            
            start_time = time.time()
            
            while time.time() - start_time < self.timeout:
                try:
                    data, addr = self.socket.recvfrom(1024)
                    
                    # Deserializar pacote
                    try:
                        packet = RDT21Packet.deserialize(data)
                    except PacketError:
                        # Compatibilidade com RDT20
                        from utils.packet import RDT20Packet
                        rdt20_packet = RDT20Packet.deserialize(data)
                        packet = RDT21Packet(rdt20_packet.type, 0, rdt20_packet.data, rdt20_packet.checksum)
                    
                    # Verificar se é ACK válido
                    if (packet.is_ack_packet() and 
                        not packet.is_corrupted() and 
                        hasattr(packet, 'seq_num') and 
                        packet.seq_num == self.seq_num):
                        
                        self.logger.log_reception("ACK", seq_num=packet.seq_num, success=True)
                        return True
                    
                except socket.timeout:
                    break
                except Exception as e:
                    print(f"Erro ao receber ACK: {e}")
                    continue
            
            # Timeout
            return False
            
        except Exception as e:
            print(f"Erro no wait_for_ack: {e}")
            return False
        finally:
            # Restaurar timeout original
            self.socket.settimeout(original_timeout)
    
    def reset(self):
        """Reseta o sender para estado inicial."""
        super().reset()


# RDT30Receiver é idêntico ao RDT21Receiver (compatibilidade total)
class RDT30Receiver(RDT21Receiver):
    """
    Receptor RDT 3.0 mantendo exata compatibilidade com RDT 2.1.
    
    O protocolo RDT 3.0 adiciona apenas funcionalidades no sender (timer de timeout).
    O receiver não precisa de modificações pois:
    - Continua usando os mesmos formatos de pacote RDT21
    - Mantém a mesma lógica de números de sequência
    - Envia os mesmos tipos de ACK/NAK
    - Não precisa saber sobre timers do sender
    
    Esta classe serve como alias documentado do RDT21Receiver para garantir
    interoperabilidade total entre RDT30Sender e RDT21Receiver.
    """
    
    def __init__(self, socket_obj: socket.socket, channel: UnreliableChannel,
                 logger: Optional[ProtocolLogger] = None):
        """
        Inicializa o receptor RDT 3.0.
        
        Args:
            socket_obj: Socket UDP para comunicação
            channel: Canal não confiável para simulação
            logger: Logger para coleta de métricas (opcional)
        """
        super().__init__(socket_obj, channel, logger)
        
        # Atualizar logger para RDT30
        if logger is None:
            self.logger = ProtocolLogger("RDT30Receiver")
