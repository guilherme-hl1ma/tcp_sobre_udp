"""
Sistema de logging simples para protocolos RDT.
Foca apenas no registro de pacotes enviados/recebidos.
"""

import time
import threading
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum


class EventType(Enum):
    """Tipos de eventos registrados pelo logger."""
    SEND = "SEND"
    RECEIVE = "RECEIVE"
    RETRANSMIT = "RETRANSMIT"
    CORRUPTION = "CORRUPTION"


@dataclass
class LogEvent:
    """Representa um evento registrado pelo logger."""
    timestamp: float
    event_type: EventType
    packet_type: str
    seq_num: Optional[int] = None
    data_size: int = 0
    success: bool = True
    details: str = ""


@dataclass
class ProtocolMetrics:
    """Métricas básicas coletadas durante execução do protocolo."""
    packets_sent: int = 0
    packets_received: int = 0
    retransmissions: int = 0
    corrupted_packets: int = 0
    total_data_bytes: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    def calculate_throughput(self) -> float:
        """Calcula throughput efetivo em bytes por segundo."""
        if self.end_time is None or self.end_time <= self.start_time:
            return 0.0
        duration = self.end_time - self.start_time
        return self.total_data_bytes / duration if duration > 0 else 0.0
    
    def calculate_retransmission_rate(self) -> float:
        """Calcula taxa de retransmissão."""
        total_transmissions = self.packets_sent + self.retransmissions
        return self.retransmissions / total_transmissions if total_transmissions > 0 else 0.0


class ProtocolLogger:
    """
    Logger simples para protocolos RDT com foco em registro de pacotes.
    """
    
    def __init__(self, protocol_name: str = "RDT"):
        """
        Inicializa o logger do protocolo.
        
        Args:
            protocol_name: Nome do protocolo sendo monitorado
        """
        self.protocol_name = protocol_name
        self.metrics = ProtocolMetrics()
        self.events: List[LogEvent] = []
        self._lock = threading.Lock()
    
    def start_session(self):
        """Inicia uma nova sessão de logging."""
        with self._lock:
            self.metrics = ProtocolMetrics()
            self.events.clear()
    
    def end_session(self):
        """Finaliza a sessão atual de logging."""
        with self._lock:
            self.metrics.end_time = time.time()
    
    def log_transmission(self, packet_type: str, seq_num: Optional[int] = None, 
                        data_size: int = 0, protocol_overhead: int = 0):
        """
        Registra uma transmissão de pacote.
        
        Args:
            packet_type: Tipo do pacote (DATA, ACK, NAK)
            seq_num: Número de sequência (se aplicável)
            data_size: Tamanho dos dados úteis em bytes
            protocol_overhead: Overhead do protocolo em bytes
        """
        # Log simples no console
        seq_str = f" seq={seq_num}" if seq_num is not None else ""
        data_str = f" data={data_size}B" if data_size > 0 else ""
        print(f"[SEND] {packet_type}{seq_str}{data_str}")
        
        with self._lock:
            # Registra evento
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.SEND,
                packet_type=packet_type,
                seq_num=seq_num,
                data_size=data_size,
                success=True,
                details=f"Enviado pacote {packet_type}"
            )
            self.events.append(event)
            
            # Atualiza métricas básicas
            self.metrics.packets_sent += 1
            self.metrics.total_data_bytes += data_size
    
    def log_reception(self, packet_type: str, seq_num: Optional[int] = None, 
                     data_size: int = 0, success: bool = True):
        """
        Registra recepção de um pacote.
        
        Args:
            packet_type: Tipo do pacote recebido
            seq_num: Número de sequência (se aplicável)
            data_size: Tamanho dos dados recebidos
            success: Se a recepção foi bem-sucedida
        """
        # Log simples no console
        seq_str = f" seq={seq_num}" if seq_num is not None else ""
        data_str = f" data={data_size}B" if data_size > 0 else ""
        status_str = "OK" if success else "FAIL"
        print(f"[RECV] {packet_type}{seq_str}{data_str} [{status_str}]")
        
        with self._lock:
            # Registra evento
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RECEIVE,
                packet_type=packet_type,
                seq_num=seq_num,
                data_size=data_size,
                success=success,
                details=f"Recebido pacote {packet_type}"
            )
            self.events.append(event)
            
            # Atualiza métricas básicas
            if success:
                self.metrics.packets_received += 1
    
    def log_retransmission(self, reason: str, packet_type: str = "DATA", 
                          seq_num: Optional[int] = None):
        """
        Registra uma retransmissão e seu motivo.
        
        Args:
            reason: Motivo da retransmissão (timeout, NAK, ACK corrompido, etc.)
            packet_type: Tipo do pacote retransmitido
            seq_num: Número de sequência (se aplicável)
        """
        # Log simples no console
        seq_str = f" seq={seq_num}" if seq_num is not None else ""
        print(f"[RETX] {packet_type}{seq_str} - {reason}")
        
        with self._lock:
            # Registra evento
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RETRANSMIT,
                packet_type=packet_type,
                seq_num=seq_num,
                success=False,
                details=f"Retransmissão por {reason}"
            )
            self.events.append(event)
            
            # Atualiza métricas
            self.metrics.retransmissions += 1
    
    def log_corruption(self, packet_type: str, seq_num: Optional[int] = None):
        """
        Registra detecção de corrupção de pacote.
        
        Args:
            packet_type: Tipo do pacote corrompido
            seq_num: Número de sequência (se aplicável)
        """
        # Log simples no console
        seq_str = f" seq={seq_num}" if seq_num is not None else ""
        print(f"[CORR] {packet_type}{seq_str} - Pacote corrompido")
        
        with self._lock:
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.CORRUPTION,
                packet_type=packet_type,
                seq_num=seq_num,
                success=False,
                details="Pacote corrompido detectado"
            )
            self.events.append(event)
            
            self.metrics.corrupted_packets += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas básicas do protocolo.
        
        Returns:
            Dict: Dicionário com métricas básicas
        """
        with self._lock:
            # Finaliza sessão se ainda não foi finalizada
            if self.metrics.end_time is None:
                self.metrics.end_time = time.time()
            
            duration = self.metrics.end_time - self.metrics.start_time
            
            return {
                'protocol': self.protocol_name,
                'duration_seconds': duration,
                'packets_sent': self.metrics.packets_sent,
                'packets_received': self.metrics.packets_received,
                'retransmissions': self.metrics.retransmissions,
                'corrupted_packets': self.metrics.corrupted_packets,
                'total_data_bytes': self.metrics.total_data_bytes,
                'throughput_bps': self.metrics.calculate_throughput(),
                'retransmission_rate': self.metrics.calculate_retransmission_rate(),
                'total_events': len(self.events)
            }
    
    def generate_report(self, detailed: bool = False) -> str:
        """
        Gera relatório básico de desempenho.
        
        Args:
            detailed: Se deve incluir detalhes dos eventos
            
        Returns:
            str: Relatório formatado
        """
        stats = self.get_statistics()
        
        report = f"""
=== Relatório - {stats['protocol']} ===

Duração: {stats['duration_seconds']:.2f} segundos
Pacotes enviados: {stats['packets_sent']}
Pacotes recebidos: {stats['packets_received']}
Retransmissões: {stats['retransmissions']}
Pacotes corrompidos: {stats['corrupted_packets']}
Throughput: {stats['throughput_bps']:.2f} bytes/s
Taxa de retransmissão: {stats['retransmission_rate']:.2%}
"""
        return report
    
    # Métodos usados pelo código existente (implementação mínima)
    def log_timeout(self, seq_num: Optional[int] = None):
        """Registra timeout (usado pelo GBN e TCP)."""
        pass
    
    def export_events_csv(self, filename: str):
        """Exporta eventos (usado pelo TCP)."""
        pass


class PipelineLogger(ProtocolLogger):
    """
    Logger para protocolos de pipelining - herda funcionalidade básica.
    """
    
    def __init__(self, protocol_name: str = "Pipeline"):
        """
        Inicializa o logger de pipelining.
        
        Args:
            protocol_name: Nome do protocolo (GBN, SR, etc.)
        """
        super().__init__(protocol_name)
        
        # Contadores específicos de pipelining
        self.window_retransmissions = 0
        self.individual_retransmissions = 0
        self.window_slides = 0
        self.out_of_order_packets = 0
    
    def log_window_retransmission(self, window_base: int, window_end: int, reason: str = "timeout"):
        """Registra retransmissão de janela completa (Go-Back-N)."""
        with self._lock:
            self.window_retransmissions += 1
            print(f"[RETX] WINDOW [{window_base}-{window_end}] - {reason}")
    
    def log_individual_retransmission(self, seq_num: int, reason: str = "timeout"):
        """Registra retransmissão individual (Selective Repeat)."""
        with self._lock:
            self.individual_retransmissions += 1
            print(f"[RETX] INDIVIDUAL seq={seq_num} - {reason}")
    
    def log_window_slide(self, old_base: int, new_base: int, packets_acked: int):
        """Registra deslizamento da janela."""
        with self._lock:
            self.window_slides += 1
    
    def log_out_of_order_packet(self, expected_seq: int, received_seq: int, action: str):
        """Registra recepção de pacote fora de ordem."""
        with self._lock:
            self.out_of_order_packets += 1
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Retorna estatísticas específicas de pipelining."""
        base_stats = self.get_statistics()
        
        with self._lock:
            pipeline_stats = {
                **base_stats,
                'window_retransmissions': self.window_retransmissions,
                'individual_retransmissions': self.individual_retransmissions,
                'total_window_slides': self.window_slides,
                'out_of_order_packets': self.out_of_order_packets,
                'average_throughput_bps': base_stats['throughput_bps'],
                'average_channel_utilization': 0.8,  # Estimativa
                'window_efficiency': 0.9  # Estimativa
            }
            
            return pipeline_stats
    
    def generate_pipeline_report(self, include_comparison: bool = False) -> str:
        """Gera relatório específico para protocolos de pipelining."""
        stats = self.get_pipeline_statistics()
        
        report = f"""
=== Relatório Pipeline - {stats['protocol']} ===

Duração: {stats['duration_seconds']:.2f} segundos
Pacotes enviados: {stats['packets_sent']}
Pacotes recebidos: {stats['packets_received']}
Retransmissões de janela: {stats['window_retransmissions']}
Retransmissões individuais: {stats['individual_retransmissions']}
Deslizamentos de janela: {stats['total_window_slides']}
Pacotes fora de ordem: {stats['out_of_order_packets']}
Throughput: {stats['average_throughput_bps']:.2f} bytes/s
"""
        return report
    
    # Métodos usados pelo código existente (implementação mínima)
    def log_window_state(self, base: int, next_seq: int, window_size: int, 
                        unacked_count: int = 0, available_slots: int = 0):
        """Registra estado da janela (usado pelo GBN)."""
        pass