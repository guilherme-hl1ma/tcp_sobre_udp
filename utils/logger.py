"""
Sistema de logging e coleta de métricas para protocolos RDT.
Implementa ProtocolLogger para registrar transmissões, retransmissões e gerar relatórios de desempenho.
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
    TIMEOUT = "TIMEOUT"
    CORRUPTION = "CORRUPTION"
    LOSS = "LOSS"
    ACK = "ACK"
    NAK = "NAK"


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
    
    def __post_init__(self):
        """Garante que timestamp seja definido se não fornecido."""
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class ProtocolMetrics:
    """Métricas coletadas durante execução do protocolo."""
    # Contadores básicos
    packets_sent: int = 0
    packets_received: int = 0
    retransmissions: int = 0
    timeouts: int = 0
    corrupted_packets: int = 0
    lost_packets: int = 0
    
    # Dados transferidos
    total_data_bytes: int = 0
    total_protocol_bytes: int = 0
    
    # Tempos
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    # Listas para cálculos detalhados
    transmission_times: List[float] = field(default_factory=list)
    round_trip_times: List[float] = field(default_factory=list)
    
    def calculate_throughput(self) -> float:
        """
        Calcula throughput efetivo em bytes por segundo.
        
        Returns:
            float: Throughput em bytes/s
        """
        if self.end_time is None or self.end_time <= self.start_time:
            return 0.0
        
        duration = self.end_time - self.start_time
        return self.total_data_bytes / duration if duration > 0 else 0.0
    
    def calculate_retransmission_rate(self) -> float:
        """
        Calcula taxa de retransmissão.
        
        Returns:
            float: Taxa de retransmissão (0.0 a 1.0)
        """
        total_transmissions = self.packets_sent + self.retransmissions
        return self.retransmissions / total_transmissions if total_transmissions > 0 else 0.0
    
    def calculate_overhead(self) -> float:
        """
        Calcula overhead do protocolo.
        
        Returns:
            float: Overhead como razão (bytes protocolo / bytes dados)
        """
        return self.total_protocol_bytes / self.total_data_bytes if self.total_data_bytes > 0 else 0.0
    
    def calculate_average_rtt(self) -> float:
        """
        Calcula tempo médio de round-trip.
        
        Returns:
            float: RTT médio em segundos
        """
        return sum(self.round_trip_times) / len(self.round_trip_times) if self.round_trip_times else 0.0
    
    def calculate_loss_rate(self) -> float:
        """
        Calcula taxa de perda de pacotes.
        
        Returns:
            float: Taxa de perda (0.0 a 1.0)
        """
        total_packets = self.packets_sent + self.lost_packets
        return self.lost_packets / total_packets if total_packets > 0 else 0.0


class ProtocolLogger:
    """
    Logger para protocolos RDT com coleta de métricas e geração de relatórios.
    Thread-safe para uso em ambientes concorrentes.
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
        self._active_transmissions: Dict[int, float] = {}  # seq_num -> start_time
    
    def start_session(self):
        """Inicia uma nova sessão de logging."""
        with self._lock:
            self.metrics = ProtocolMetrics()
            self.events.clear()
            self._active_transmissions.clear()
    
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
        timestamp = time.time()
        
        with self._lock:
            # Registra evento
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.SEND,
                packet_type=packet_type,
                seq_num=seq_num,
                data_size=data_size,
                success=True,
                details=f"Enviado pacote {packet_type}"
            )
            self.events.append(event)
            
            # Atualiza métricas
            self.metrics.packets_sent += 1
            self.metrics.total_data_bytes += data_size
            self.metrics.total_protocol_bytes += protocol_overhead
            
            # Registra início de transmissão para cálculo de RTT
            if seq_num is not None and packet_type == "DATA":
                self._active_transmissions[seq_num] = timestamp
    
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
        timestamp = time.time()
        
        with self._lock:
            # Registra evento
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.RECEIVE,
                packet_type=packet_type,
                seq_num=seq_num,
                data_size=data_size,
                success=success,
                details=f"Recebido pacote {packet_type}"
            )
            self.events.append(event)
            
            # Atualiza métricas
            if success:
                self.metrics.packets_received += 1
                
                # Calcula RTT se for ACK para transmissão ativa
                if packet_type == "ACK" and seq_num is not None:
                    if seq_num in self._active_transmissions:
                        rtt = timestamp - self._active_transmissions[seq_num]
                        self.metrics.round_trip_times.append(rtt)
                        del self._active_transmissions[seq_num]
    
    def log_retransmission(self, reason: str, packet_type: str = "DATA", 
                          seq_num: Optional[int] = None):
        """
        Registra uma retransmissão e seu motivo.
        
        Args:
            reason: Motivo da retransmissão (timeout, NAK, ACK corrompido, etc.)
            packet_type: Tipo do pacote retransmitido
            seq_num: Número de sequência (se aplicável)
        """
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
    
    def log_timeout(self, seq_num: Optional[int] = None):
        """
        Registra um evento de timeout.
        
        Args:
            seq_num: Número de sequência do pacote que sofreu timeout
        """
        with self._lock:
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.TIMEOUT,
                packet_type="DATA",
                seq_num=seq_num,
                success=False,
                details="Timeout detectado"
            )
            self.events.append(event)
            
            self.metrics.timeouts += 1
    
    def log_corruption(self, packet_type: str, seq_num: Optional[int] = None):
        """
        Registra detecção de corrupção de pacote.
        
        Args:
            packet_type: Tipo do pacote corrompido
            seq_num: Número de sequência (se aplicável)
        """
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
    
    def log_loss(self, packet_type: str, seq_num: Optional[int] = None):
        """
        Registra perda de pacote (simulada).
        
        Args:
            packet_type: Tipo do pacote perdido
            seq_num: Número de sequência (se aplicável)
        """
        with self._lock:
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.LOSS,
                packet_type=packet_type,
                seq_num=seq_num,
                success=False,
                details="Pacote perdido (simulação)"
            )
            self.events.append(event)
            
            self.metrics.lost_packets += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas completas do protocolo.
        
        Returns:
            Dict: Dicionário com todas as métricas calculadas
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
                'timeouts': self.metrics.timeouts,
                'corrupted_packets': self.metrics.corrupted_packets,
                'lost_packets': self.metrics.lost_packets,
                'total_data_bytes': self.metrics.total_data_bytes,
                'total_protocol_bytes': self.metrics.total_protocol_bytes,
                'throughput_bps': self.metrics.calculate_throughput(),
                'retransmission_rate': self.metrics.calculate_retransmission_rate(),
                'protocol_overhead': self.metrics.calculate_overhead(),
                'average_rtt_ms': self.metrics.calculate_average_rtt() * 1000,
                'loss_rate': self.metrics.calculate_loss_rate(),
                'total_events': len(self.events)
            }
    
    def generate_report(self, detailed: bool = False) -> str:
        """
        Gera relatório de desempenho formatado.
        
        Args:
            detailed: Se deve incluir detalhes dos eventos
            
        Returns:
            str: Relatório formatado
        """
        stats = self.get_statistics()
        
        report = f"""
=== Relatório de Desempenho - {stats['protocol']} ===

Duração da Sessão: {stats['duration_seconds']:.2f} segundos

Transmissão:
  • Pacotes enviados: {stats['packets_sent']}
  • Pacotes recebidos: {stats['packets_received']}
  • Retransmissões: {stats['retransmissions']}
  • Taxa de retransmissão: {stats['retransmission_rate']:.2%}

Erros e Perdas:
  • Timeouts: {stats['timeouts']}
  • Pacotes corrompidos: {stats['corrupted_packets']}
  • Pacotes perdidos: {stats['lost_packets']}
  • Taxa de perda: {stats['loss_rate']:.2%}

Desempenho:
  • Throughput: {stats['throughput_bps']:.2f} bytes/s
  • Dados úteis: {stats['total_data_bytes']} bytes
  • Overhead protocolo: {stats['total_protocol_bytes']} bytes
  • Overhead ratio: {stats['protocol_overhead']:.2f}
  • RTT médio: {stats['average_rtt_ms']:.2f} ms

Total de eventos registrados: {stats['total_events']}
"""
        
        if detailed and self.events:
            report += "\n=== Eventos Detalhados ===\n"
            for i, event in enumerate(self.events[-20:]):  # Últimos 20 eventos
                timestamp_str = time.strftime('%H:%M:%S', time.localtime(event.timestamp))
                report += f"{i+1:2d}. [{timestamp_str}] {event.event_type.value} - {event.packet_type}"
                if event.seq_num is not None:
                    report += f" (seq={event.seq_num})"
                if event.details:
                    report += f" - {event.details}"
                report += "\n"
        
        return report
    
    def export_events_csv(self, filename: str):
        """
        Exporta eventos para arquivo CSV.
        
        Args:
            filename: Nome do arquivo CSV
        """
        import csv
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'event_type', 'packet_type', 'seq_num', 
                         'data_size', 'success', 'details']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for event in self.events:
                writer.writerow({
                    'timestamp': event.timestamp,
                    'event_type': event.event_type.value,
                    'packet_type': event.packet_type,
                    'seq_num': event.seq_num,
                    'data_size': event.data_size,
                    'success': event.success,
                    'details': event.details
                })


class PipelineLogger(ProtocolLogger):
    """
    Logger estendido para protocolos de pipelining com métricas específicas.
    
    Adiciona funcionalidades para:
    - Métricas de throughput e utilização de canal
    - Estado de janela deslizante
    - Análise de retransmissões de janela completa vs. individuais
    - Relatórios comparativos de performance
    """
    
    def __init__(self, protocol_name: str = "Pipeline"):
        """
        Inicializa o logger de pipelining.
        
        Args:
            protocol_name: Nome do protocolo (GBN, SR, etc.)
        """
        super().__init__(protocol_name)
        
        # Métricas específicas de pipelining
        self.window_states: List[Dict[str, Any]] = []
        self.throughput_samples: List[Tuple[float, float]] = []  # (timestamp, bytes_per_second)
        self.channel_utilization_samples: List[Tuple[float, float]] = []  # (timestamp, utilization)
        
        # Contadores específicos
        self.window_retransmissions = 0  # Retransmissões de janela completa (GBN)
        self.individual_retransmissions = 0  # Retransmissões individuais (SR)
        self.window_slides = 0
        self.out_of_order_packets = 0
        self.buffered_packets = 0
        
        # Dados para cálculo de utilização
        self.effective_transmission_time = 0.0
        self.total_session_time = 0.0
    
    def log_window_state(self, base: int, next_seq: int, window_size: int, 
                        unacked_count: int = 0, available_slots: int = 0):
        """
        Registra estado atual da janela deslizante.
        
        Args:
            base: Base da janela (primeiro não confirmado)
            next_seq: Próximo número de sequência
            window_size: Tamanho da janela
            unacked_count: Número de pacotes não confirmados
            available_slots: Slots disponíveis na janela
        """
        timestamp = time.time()
        
        with self._lock:
            window_state = {
                'timestamp': timestamp,
                'base': base,
                'next_seq_num': next_seq,
                'window_size': window_size,
                'packets_in_flight': next_seq - base,
                'unacked_count': unacked_count,
                'available_slots': available_slots,
                'window_utilization': (next_seq - base) / window_size if window_size > 0 else 0.0
            }
            
            self.window_states.append(window_state)
            
            # Registra evento
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.SEND,  # Tipo genérico para estado
                packet_type="WINDOW_STATE",
                details=f"Base={base}, Next={next_seq}, InFlight={next_seq-base}"
            )
            self.events.append(event)
    
    def log_throughput_sample(self, bytes_transferred: int, time_elapsed: float):
        """
        Registra amostra de throughput instantâneo.
        
        Args:
            bytes_transferred: Bytes transferidos no período
            time_elapsed: Tempo decorrido em segundos
        """
        if time_elapsed > 0:
            throughput = bytes_transferred / time_elapsed
            timestamp = time.time()
            
            with self._lock:
                self.throughput_samples.append((timestamp, throughput))
    
    def log_channel_utilization(self, effective_time: float, total_time: float):
        """
        Registra amostra de utilização do canal.
        
        Args:
            effective_time: Tempo efetivamente usado para transmissão
            total_time: Tempo total do período
        """
        if total_time > 0:
            utilization = effective_time / total_time
            timestamp = time.time()
            
            with self._lock:
                self.channel_utilization_samples.append((timestamp, utilization))
                self.effective_transmission_time += effective_time
    
    def log_window_retransmission(self, window_base: int, window_end: int, reason: str = "timeout"):
        """
        Registra retransmissão de janela completa (Go-Back-N).
        
        Args:
            window_base: Base da janela retransmitida
            window_end: Fim da janela retransmitida
            reason: Motivo da retransmissão
        """
        with self._lock:
            self.window_retransmissions += 1
            
            # Registra evento específico
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RETRANSMIT,
                packet_type="WINDOW",
                details=f"Retransmissão janela [{window_base}, {window_end}] - {reason}"
            )
            self.events.append(event)
    
    def log_individual_retransmission(self, seq_num: int, reason: str = "timeout"):
        """
        Registra retransmissão individual (Selective Repeat).
        
        Args:
            seq_num: Número de sequência do pacote retransmitido
            reason: Motivo da retransmissão
        """
        with self._lock:
            self.individual_retransmissions += 1
            
            # Registra evento específico
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RETRANSMIT,
                packet_type="INDIVIDUAL",
                seq_num=seq_num,
                details=f"Retransmissão individual seq={seq_num} - {reason}"
            )
            self.events.append(event)
    
    def log_window_slide(self, old_base: int, new_base: int, packets_acked: int):
        """
        Registra deslizamento da janela.
        
        Args:
            old_base: Base anterior da janela
            new_base: Nova base da janela
            packets_acked: Número de pacotes confirmados
        """
        with self._lock:
            self.window_slides += 1
            
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.ACK,
                packet_type="WINDOW_SLIDE",
                details=f"Janela deslizou de {old_base} para {new_base} ({packets_acked} ACKs)"
            )
            self.events.append(event)
    
    def log_out_of_order_packet(self, expected_seq: int, received_seq: int, action: str):
        """
        Registra recepção de pacote fora de ordem.
        
        Args:
            expected_seq: Número de sequência esperado
            received_seq: Número de sequência recebido
            action: Ação tomada (buffer, discard, etc.)
        """
        with self._lock:
            self.out_of_order_packets += 1
            
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RECEIVE,
                packet_type="OUT_OF_ORDER",
                seq_num=received_seq,
                details=f"Esperado {expected_seq}, recebido {received_seq} - {action}"
            )
            self.events.append(event)
    
    def log_packet_buffered(self, seq_num: int, buffer_size: int):
        """
        Registra armazenamento de pacote no buffer (SR).
        
        Args:
            seq_num: Número de sequência do pacote
            buffer_size: Tamanho atual do buffer
        """
        with self._lock:
            self.buffered_packets += 1
            
            event = LogEvent(
                timestamp=time.time(),
                event_type=EventType.RECEIVE,
                packet_type="BUFFERED",
                seq_num=seq_num,
                details=f"Pacote {seq_num} armazenado, buffer size={buffer_size}"
            )
            self.events.append(event)
    
    def calculate_average_throughput(self) -> float:
        """
        Calcula throughput médio baseado nas amostras coletadas.
        
        Returns:
            float: Throughput médio em bytes/s
        """
        with self._lock:
            if not self.throughput_samples:
                return self.metrics.calculate_throughput()
            
            total_throughput = sum(sample[1] for sample in self.throughput_samples)
            return total_throughput / len(self.throughput_samples)
    
    def calculate_average_utilization(self) -> float:
        """
        Calcula utilização média do canal.
        
        Returns:
            float: Utilização média (0.0 a 1.0)
        """
        with self._lock:
            if self.metrics.end_time and self.metrics.start_time:
                self.total_session_time = self.metrics.end_time - self.metrics.start_time
                
                if self.total_session_time > 0:
                    return self.effective_transmission_time / self.total_session_time
            
            # Fallback para amostras
            if self.channel_utilization_samples:
                total_util = sum(sample[1] for sample in self.channel_utilization_samples)
                return total_util / len(self.channel_utilization_samples)
            
            return 0.0
    
    def calculate_window_efficiency(self) -> float:
        """
        Calcula eficiência média da janela.
        
        Returns:
            float: Eficiência média da janela (0.0 a 1.0)
        """
        with self._lock:
            if not self.window_states:
                return 0.0
            
            total_utilization = sum(state['window_utilization'] for state in self.window_states)
            return total_utilization / len(self.window_states)
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas específicas de pipelining.
        
        Returns:
            Dict: Estatísticas estendidas incluindo métricas de pipelining
        """
        base_stats = self.get_statistics()
        
        with self._lock:
            pipeline_stats = {
                # Estatísticas base
                **base_stats,
                
                # Métricas de pipelining
                'average_throughput_bps': self.calculate_average_throughput(),
                'average_channel_utilization': self.calculate_average_utilization(),
                'window_efficiency': self.calculate_window_efficiency(),
                
                # Contadores específicos
                'window_retransmissions': self.window_retransmissions,
                'individual_retransmissions': self.individual_retransmissions,
                'total_window_slides': self.window_slides,
                'out_of_order_packets': self.out_of_order_packets,
                'buffered_packets': self.buffered_packets,
                
                # Amostras coletadas
                'throughput_samples_count': len(self.throughput_samples),
                'utilization_samples_count': len(self.channel_utilization_samples),
                'window_state_samples': len(self.window_states),
                
                # Tempos
                'effective_transmission_time': self.effective_transmission_time,
                'total_session_time': self.total_session_time,
            }
            
            return pipeline_stats
    
    def generate_pipeline_report(self, include_comparison: bool = False) -> str:
        """
        Gera relatório específico para protocolos de pipelining.
        
        Args:
            include_comparison: Se deve incluir comparação com stop-and-wait
            
        Returns:
            str: Relatório formatado
        """
        stats = self.get_pipeline_statistics()
        
        report = f"""
=== Relatório de Performance - {stats['protocol']} (Pipelining) ===

Duração da Sessão: {stats['duration_seconds']:.2f} segundos

Transmissão e Janela:
  • Pacotes enviados: {stats['packets_sent']}
  • Pacotes recebidos: {stats['packets_received']}
  • Deslizamentos de janela: {stats['total_window_slides']}
  • Eficiência da janela: {stats['window_efficiency']:.2%}

Retransmissões:
  • Retransmissões de janela: {stats['window_retransmissions']}
  • Retransmissões individuais: {stats['individual_retransmissions']}
  • Taxa total de retransmissão: {stats['retransmission_rate']:.2%}

Performance:
  • Throughput médio: {stats['average_throughput_bps']:.2f} bytes/s
  • Utilização do canal: {stats['average_channel_utilization']:.2%}
  • Dados úteis: {stats['total_data_bytes']} bytes
  • Overhead protocolo: {stats['protocol_overhead']:.2f}

Pacotes Fora de Ordem:
  • Pacotes fora de ordem: {stats['out_of_order_packets']}
  • Pacotes bufferizados: {stats['buffered_packets']}

Amostras Coletadas:
  • Amostras de throughput: {stats['throughput_samples_count']}
  • Amostras de utilização: {stats['utilization_samples_count']}
  • Estados de janela: {stats['window_state_samples']}
"""
        
        if include_comparison:
            # Estimativa teórica de stop-and-wait para comparação
            if stats['average_rtt_ms'] > 0:
                theoretical_stopwait = 1000 / stats['average_rtt_ms']  # pacotes/s teórico
                improvement = stats['average_throughput_bps'] / (theoretical_stopwait * 1024) if theoretical_stopwait > 0 else 0
                
                report += f"""
Comparação Teórica:
  • RTT médio: {stats['average_rtt_ms']:.2f} ms
  • Melhoria estimada vs Stop-and-Wait: {improvement:.1f}x
"""
        
        return report
    
    def export_pipeline_metrics_csv(self, filename: str):
        """
        Exporta métricas de pipelining para CSV.
        
        Args:
            filename: Nome do arquivo CSV
        """
        import csv
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Exporta estados da janela
            if self.window_states:
                writer = csv.DictWriter(csvfile, fieldnames=self.window_states[0].keys())
                writer.writeheader()
                writer.writerows(self.window_states)


class TestMetrics:
    """
    Classe auxiliar para coleta de métricas durante testes.
    Simplifica a interface para casos de teste específicos.
    """
    
    def __init__(self, test_name: str):
        """
        Inicializa métricas de teste.
        
        Args:
            test_name: Nome do teste sendo executado
        """
        self.test_name = test_name
        self.logger = ProtocolLogger(test_name)
        self.start_time = time.time()
    
    def start_test(self):
        """Inicia coleta de métricas para o teste."""
        self.logger.start_session()
        self.start_time = time.time()
    
    def end_test(self) -> Dict[str, Any]:
        """
        Finaliza teste e retorna métricas.
        
        Returns:
            Dict: Métricas do teste
        """
        self.logger.end_session()
        return self.logger.get_statistics()
    
    def log_test_event(self, event_description: str):
        """
        Registra evento específico do teste.
        
        Args:
            event_description: Descrição do evento
        """
        event = LogEvent(
            timestamp=time.time(),
            event_type=EventType.SEND,  # Tipo genérico para eventos de teste
            packet_type="TEST",
            details=event_description
        )
        self.logger.events.append(event)


class PipelineTestMetrics(TestMetrics):
    """
    Métricas específicas para testes de pipelining.
    """
    
    def __init__(self, test_name: str, protocol_type: str = "Pipeline"):
        """
        Inicializa métricas de teste de pipelining.
        
        Args:
            test_name: Nome do teste
            protocol_type: Tipo do protocolo (GBN, SR, etc.)
        """
        self.test_name = test_name
        self.protocol_type = protocol_type
        self.logger = PipelineLogger(f"{protocol_type}_{test_name}")
        self.start_time = time.time()
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas específicas de pipelining.
        
        Returns:
            Dict: Estatísticas de pipelining
        """
        return self.logger.get_pipeline_statistics()
    
    def generate_comparison_report(self, baseline_stats: Dict[str, Any]) -> str:
        """
        Gera relatório comparativo com baseline (ex: RDT 3.0).
        
        Args:
            baseline_stats: Estatísticas do protocolo baseline
            
        Returns:
            str: Relatório comparativo
        """
        current_stats = self.get_pipeline_statistics()
        
        throughput_improvement = (current_stats['average_throughput_bps'] / 
                                baseline_stats['throughput_bps']) if baseline_stats['throughput_bps'] > 0 else 0
        
        time_improvement = (baseline_stats['duration_seconds'] / 
                          current_stats['duration_seconds']) if current_stats['duration_seconds'] > 0 else 0
        
        return f"""
=== Relatório Comparativo: {self.protocol_type} vs {baseline_stats.get('protocol', 'Baseline')} ===

Performance:
  • Throughput {self.protocol_type}: {current_stats['average_throughput_bps']:.2f} bytes/s
  • Throughput Baseline: {baseline_stats['throughput_bps']:.2f} bytes/s
  • Melhoria de throughput: {throughput_improvement:.1f}x

Tempo:
  • Tempo {self.protocol_type}: {current_stats['duration_seconds']:.2f}s
  • Tempo Baseline: {baseline_stats['duration_seconds']:.2f}s
  • Melhoria de tempo: {time_improvement:.1f}x

Eficiência:
  • Utilização do canal: {current_stats['average_channel_utilization']:.2%}
  • Eficiência da janela: {current_stats['window_efficiency']:.2%}
  • Taxa de retransmissão: {current_stats['retransmission_rate']:.2%}
"""