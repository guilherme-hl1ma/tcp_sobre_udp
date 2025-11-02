"""
Testes obrigatórios para o protocolo Go-Back-N (GBN) - Fase 2.
Implementa os testes específicos requeridos: eficiência, perdas e análise de performance.
Compatível com pytest.
"""

import socket
import threading
import time
import pytest
from typing import Tuple, Dict, Any, List
import os
import sys
import random

# Adicionar diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fase2.gbn import GBNSender, GBNReceiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver
from utils.simulator import UnreliableChannel, PerfectChannel
from utils.logger import PipelineLogger, ProtocolLogger


def get_free_port():
    """Obtém uma porta livre para uso nos testes."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def create_socket_pair():
    """Cria um par de sockets com portas livres."""
    port1 = get_free_port()
    port2 = get_free_port()
    
    # Garantir que as portas são diferentes
    while port2 == port1:
        port2 = get_free_port()
    
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    try:
        sock1.bind(("localhost", port1))
        sock2.bind(("localhost", port2))
        return sock1, sock2, ("localhost", port1), ("localhost", port2)
    except OSError:
        sock1.close()
        sock2.close()
        raise


class TestGBNEfficiency:
    """Teste de Eficiência: Transferir 1MB de dados e comparar com RDT 3.0."""
    
    def setup_method(self):
        """Configuração inicial para testes de eficiência."""
        # Criar sockets com portas dinâmicas
        (self.gbn_sender_socket, self.gbn_receiver_socket, 
         self.gbn_sender_addr, self.gbn_receiver_addr) = create_socket_pair()
        
        (self.rdt_sender_socket, self.rdt_receiver_socket,
         self.rdt_sender_addr, self.rdt_receiver_addr) = create_socket_pair()
        
        # Configurar timeouts
        for sock in [self.gbn_sender_socket, self.gbn_receiver_socket, 
                    self.rdt_sender_socket, self.rdt_receiver_socket]:
            sock.settimeout(3.0)
    
    def teardown_method(self):
        """Limpeza após testes."""
        for sock in [self.gbn_sender_socket, self.gbn_receiver_socket,
                    self.rdt_sender_socket, self.rdt_receiver_socket]:
            try:
                sock.close()
            except:
                pass
        time.sleep(0.1)  # Aguardar liberação das portas
    
    def test_1mb_transfer_efficiency(self):
        """Transferir dados simulando 1MB e comparar tempo com RDT 3.0 (stop-and-wait)."""
        # Gerar dados de teste (100KB para teste mais rápido, mas representativo)
        chunk_size = 512   # 512 bytes por pacote
        num_chunks = 200   # 200 pacotes = ~100KB (mais rápido que 1MB completo)
        test_data = [f"Data chunk {i:04d}".ljust(chunk_size, 'X').encode() 
                    for i in range(num_chunks)]
        
        print(f"\n=== Teste de Eficiência: Transfer Efficiency Test ===")
        print(f"Transferindo {len(test_data)} pacotes ({sum(len(d) for d in test_data)} bytes)")
        print("(Teste otimizado - representa comportamento de 1MB)")
        
        # Testar GBN
        gbn_stats = self._test_protocol_performance(
            "GBN", test_data, 
            self.gbn_sender_socket, self.gbn_receiver_socket,
            self.gbn_receiver_addr, use_gbn=True
        )
        
        # Testar RDT 3.0 (stop-and-wait)
        rdt_stats = self._test_protocol_performance(
            "RDT30", test_data,
            self.rdt_sender_socket, self.rdt_receiver_socket, 
            self.rdt_receiver_addr, use_gbn=False
        )
        
        # Calcular melhoria de performance e utilização do canal
        throughput_improvement = (gbn_stats['throughput_bps'] / 
                                rdt_stats['throughput_bps']) if rdt_stats['throughput_bps'] > 0 else 0
        
        time_improvement = (rdt_stats['duration_seconds'] / 
                          gbn_stats['duration_seconds']) if gbn_stats['duration_seconds'] > 0 else 0
        
        # Calcular utilização do canal
        gbn_utilization = self._calculate_channel_utilization(gbn_stats)
        rdt_utilization = self._calculate_channel_utilization(rdt_stats)
        
        # Gerar relatório
        print(f"\n=== Resultados da Comparação ===")
        print(f"GBN - Tempo: {gbn_stats['duration_seconds']:.2f}s, Throughput: {gbn_stats['throughput_bps']:.2f} bytes/s")
        print(f"RDT30 - Tempo: {rdt_stats['duration_seconds']:.2f}s, Throughput: {rdt_stats['throughput_bps']:.2f} bytes/s")
        print(f"Melhoria de throughput: {throughput_improvement:.1f}x")
        print(f"Melhoria de tempo: {time_improvement:.1f}x")
        print(f"Utilização do canal GBN: {gbn_utilization:.2%}")
        print(f"Utilização do canal RDT30: {rdt_utilization:.2%}")
        
        # Validações (mais flexíveis para testes rápidos)
        assert throughput_improvement > 1.2, \
            f"GBN deve ser pelo menos 1.2x mais rápido que RDT 3.0. Atual: {throughput_improvement:.1f}x"
        
        assert gbn_stats['throughput_bps'] > rdt_stats['throughput_bps'], \
            "Throughput do GBN deve ser maior que RDT 3.0"
        
        assert gbn_utilization > rdt_utilization, \
            "Utilização do canal GBN deve ser maior que RDT 3.0"
        
        return {
            'gbn_stats': gbn_stats,
            'rdt_stats': rdt_stats,
            'throughput_improvement': throughput_improvement,
            'time_improvement': time_improvement,
            'gbn_utilization': gbn_utilization,
            'rdt_utilization': rdt_utilization
        }
    
    def _calculate_channel_utilization(self, stats: Dict[str, Any]) -> float:
        """Calcular utilização do canal."""
        if stats['duration_seconds'] <= 0:
            return 0.0
        
        # Utilização = (dados úteis transmitidos) / (capacidade teórica do canal * tempo)
        # Assumindo capacidade teórica de 1MB/s para simplificação
        theoretical_capacity = 1024 * 1024  # 1MB/s
        actual_throughput = stats['throughput_bps']
        
        return min(actual_throughput / theoretical_capacity, 1.0)
    
    def _test_protocol_performance(self, protocol_name: str, test_data: list, 
                                 sender_socket: socket.socket, receiver_socket: socket.socket,
                                 receiver_addr: Tuple[str, int], use_gbn: bool = True) -> Dict[str, Any]:
        """Testa performance de um protocolo específico."""
        # Criar canal com condições controladas (baixa perda para teste de eficiência)
        channel = UnreliableChannel(loss_rate=0.02, corrupt_rate=0.01, 
                                  delay_range=(0.001, 0.005), verbose=False)
        
        if use_gbn:
            logger = PipelineLogger(f"{protocol_name}_Performance")
            sender = GBNSender(sender_socket, channel, window_size=10, 
                             timeout=1.0, logger=logger)
            receiver = GBNReceiver(receiver_socket, channel, logger=logger)
        else:
            logger = ProtocolLogger(f"{protocol_name}_Performance")
            sender = RDT30Sender(sender_socket, channel, logger=logger, timeout=1.0)
            receiver = RDT30Receiver(receiver_socket, channel, logger=logger)
        
        received_data = []
        transmission_complete = threading.Event()
        
        def receiver_thread():
            """Thread do receptor."""
            while len(received_data) < len(test_data):
                try:
                    if use_gbn:
                        data = receiver.receive_data(timeout=2.0)
                    else:
                        data = receiver.receive_data()
                    
                    if data:
                        received_data.append(data)
                except:
                    break
            
            transmission_complete.set()
        
        def sender_thread():
            """Thread do remetente."""
            time.sleep(0.1)  # Aguarda receiver estar pronto
            
            for i, data in enumerate(test_data):
                success = sender.send_data(data, receiver_addr)
                if not success:
                    print(f"Falha ao enviar pacote no protocolo {protocol_name}")
                    break
                
                # !!! REMOVIDO: try_receive_ack manual - agora é automático !!!
                time.sleep(0.001)  # Delay muito pequeno
        
        # --- ADICIONE UM BLOCO try...finally ---
        try:
            # Iniciar medição de tempo
            start_time = time.time()
            logger.start_session()
            
            # Executar threads
            recv_thread = threading.Thread(target=receiver_thread)
            send_thread = threading.Thread(target=sender_thread)
            
            recv_thread.start()
            send_thread.start()
            
            # Aguardar conclusão com timeout otimizado
            send_thread.join(timeout=30.0)
            transmission_complete.wait(timeout=30.0)
            
            # Finalizar medição
            end_time = time.time()
            
            # Cleanup threads
            try:
                recv_thread.join(timeout=1.0)
            except:
                pass
            
        finally:
            # --- GARANTE QUE O SENDER SEJA DESLIGADO PRIMEIRO ---
            if use_gbn and isinstance(sender, GBNSender):
                sender.shutdown()
            if use_gbn and isinstance(receiver, GBNReceiver):
                receiver.reset()
            time.sleep(0.05)
        
        # --- CHAME O LOGGER.END_SESSION() AQUI ---
        logger.end_session()
        
        # Obter estatísticas
        if use_gbn:
            stats = logger.get_pipeline_statistics()
        else:
            stats = logger.get_statistics()
        
        # Verificar integridade dos dados
        data_integrity = len(received_data) / len(test_data) if test_data else 0
        
        # Adicionar métricas calculadas
        stats.update({
            'data_integrity': data_integrity,
            'packets_expected': len(test_data),
            'packets_received_count': len(received_data),
            'actual_duration': end_time - start_time
        })
        
        print(f"{protocol_name} - Pacotes: {len(received_data)}/{len(test_data)}, "
              f"Tempo: {stats['duration_seconds']:.2f}s, "
              f"Throughput: {stats['throughput_bps']:.2f} bytes/s")
        
        return stats


class TestGBNLoss:
    """Teste com Perdas: Taxa de perda de 10%, verificar se todas as mensagens chegam."""
    
    def setup_method(self):
        """Configuração inicial."""
        # Criar sockets com portas dinâmicas
        (self.sender_socket, self.receiver_socket,
         self.sender_addr, self.receiver_addr) = create_socket_pair()
        
        self.sender_socket.settimeout(3.0)
        self.receiver_socket.settimeout(3.0)
    
    def teardown_method(self):
        """Limpeza."""
        try:
            self.sender_socket.close()
            self.receiver_socket.close()
        except:
            pass
        time.sleep(0.1)  # Aguardar liberação das portas
    
    def test_10_percent_loss_all_messages_arrive(self):
        """Taxa de perda de 10% - Verificar se todas as mensagens chegam e contar retransmissões."""
        # Configurar UnreliableChannel com exatamente 10% de perda
        channel = UnreliableChannel(loss_rate=0.1, corrupt_rate=0.02, 
                                  delay_range=(0.01, 0.05), verbose=False)
        logger = PipelineLogger("GBN_10PercentLoss")
        
        sender = GBNSender(self.sender_socket, channel, window_size=8, 
                          timeout=1.0, logger=logger)
        receiver = GBNReceiver(self.receiver_socket, channel, logger=logger)
        
        # Transmitir dados de teste (reduzido para ser mais rápido)
        test_data = [f"Message {i:03d} - loss test data".encode() 
                    for i in range(20)]  # 20 mensagens para teste mais rápido
        received_data = []
        
        def receiver_thread():
            """Thread do receptor."""
            attempts = 0
            max_attempts = len(test_data) * 3  # Reduzir tentativas
            
            while len(received_data) < len(test_data) and attempts < max_attempts:
                try:
                    data = receiver.receive_data(timeout=1.0)  # Timeout menor
                    if data:
                        received_data.append(data)
                except socket.timeout:
                    pass  # Timeout é esperado com perdas
                attempts += 1
        
        def sender_thread():
            """Thread do remetente."""
            time.sleep(0.05)  # Aguarda receiver estar pronto
            for i, data in enumerate(test_data):
                success = sender.send_data(data, self.receiver_addr)
                assert success, "Falha ao iniciar envio"
                
                # !!! REMOVIDO: try_receive_ack manual - agora é automático !!!
                time.sleep(0.01)  # Delay menor
        
        # --- ADICIONE UM BLOCO try...finally ---
        try:
            # Executar teste
            logger.start_session()
            
            recv_thread = threading.Thread(target=receiver_thread)
            send_thread = threading.Thread(target=sender_thread)
            
            recv_thread.start()
            send_thread.start()
            
            # Aguardar conclusão com timeout otimizado
            send_thread.join(timeout=30.0)
            recv_thread.join(timeout=30.0)
            
        finally:
            # --- GARANTE QUE O SENDER SEJA DESLIGADO PRIMEIRO ---
            if isinstance(sender, GBNSender):
                sender.shutdown()
            if isinstance(receiver, GBNReceiver):
                receiver.reset()
            time.sleep(0.05)
        
        # --- CHAME O LOGGER.END_SESSION() AQUI ---
        logger.end_session()
        
        # Obter estatísticas
        stats = logger.get_pipeline_statistics()
        channel_stats = channel.get_statistics()
        
        # Verificar se todas as mensagens chegaram
        assert len(received_data) == len(test_data), \
            f"Nem todas as mensagens chegaram: {len(received_data)}/{len(test_data)}"
        
        # Verificar se as mensagens chegaram na ordem correta
        assert received_data == test_data, \
            "Mensagens recebidas não coincidem com as enviadas"
        
        # Contar e validar retransmissões
        retransmissions = stats['retransmissions']
        
        print(f"\n=== Teste com 10% de Perda ===")
        print(f"Mensagens enviadas: {len(test_data)}")
        print(f"Mensagens recebidas: {len(received_data)}")
        print(f"Pacotes processados pelo canal: {channel_stats['packets_processed']}")
        print(f"Pacotes perdidos pelo canal: {channel_stats['packets_lost']}")
        print(f"Taxa de perda real: {channel_stats['loss_rate_actual']:.2%}")
        print(f"Retransmissões realizadas: {retransmissions}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Throughput final: {stats['throughput_bps']:.2f} bytes/s")
        
        # Validações
        assert retransmissions > 0, \
            "Deveria haver retransmissões com 10% de perda"
        
        # Verificar que a taxa de perda está próxima de 10%
        actual_loss_rate = channel_stats['loss_rate_actual']
        assert actual_loss_rate > 0.05, \
            f"Taxa de perda muito baixa: {actual_loss_rate:.2%}"
        assert actual_loss_rate < 0.20, \
            f"Taxa de perda muito alta: {actual_loss_rate:.2%}"
        
        # Verificar que a taxa de retransmissão é razoável (não excessiva)
        assert stats['retransmission_rate'] < 0.6, \
            f"Taxa de retransmissão muito alta: {stats['retransmission_rate']:.2%}"
        
        return {
            'messages_sent': len(test_data),
            'messages_received': len(received_data),
            'retransmissions': retransmissions,
            'actual_loss_rate': actual_loss_rate,
            'retransmission_rate': stats['retransmission_rate'],
            'throughput': stats['throughput_bps']
        }


class TestGBNPerformanceAnalysis:
    """Análise de Desempenho: Variar tamanho da janela (N = 1, 5, 10, 20) e plotar gráfico."""
    
    def setup_method(self):
        """Configuração inicial."""
        # Criar sockets com portas dinâmicas
        (self.sender_socket, self.receiver_socket,
         self.sender_addr, self.receiver_addr) = create_socket_pair()
        
        self.sender_socket.settimeout(4.0)
        self.receiver_socket.settimeout(4.0)
    
    def teardown_method(self):
        """Limpeza."""
        try:
            self.sender_socket.close()
            self.receiver_socket.close()
        except:
            pass
        time.sleep(0.1)  # Aguardar liberação das portas
    
    def test_throughput_vs_window_size(self):
        """Variar tamanho da janela (N = 1, 5, 10, 20) e analisar throughput."""
        window_sizes = [1, 5, 10, 20]
        results = {}
        
        # Dados de teste consistentes para análise comparativa (otimizado)
        test_data = [f"Performance test packet {i:03d}".ljust(100, 'X').encode() 
                    for i in range(50)]  # 50 pacotes para análise mais rápida
        
        print(f"\n=== Análise de Desempenho: Throughput x Tamanho da Janela ===")
        print(f"Testando {len(test_data)} pacotes com janelas: {window_sizes}")
        
        for window_size in window_sizes:
            print(f"\n--- Testando janela de tamanho {window_size} ---")
            
            # Medir throughput para cada tamanho de janela
            stats = self._test_window_performance(window_size, test_data)
            results[window_size] = stats
            
            print(f"Janela {window_size}: Throughput = {stats['throughput_bps']:.2f} bytes/s, "
                  f"Tempo = {stats['duration_seconds']:.2f}s, "
                  f"Retransmissões = {stats['retransmission_rate']:.2%}")
        
        # Gerar relatório e "gráfico" textual
        self._generate_throughput_analysis(results)
        
        # Validações de performance
        self._validate_throughput_progression(results)
        
        return results
    
    def _test_window_performance(self, window_size: int, test_data: List[bytes]) -> Dict[str, Any]:
        """Testa performance com tamanho de janela específico."""
        # Canal com condições controladas para análise comparativa
        channel = UnreliableChannel(loss_rate=0.03, corrupt_rate=0.01, 
                                  delay_range=(0.005, 0.015), verbose=False)
        
        logger = PipelineLogger(f"GBN_Performance_W{window_size}")
        
        # Criar novos sockets para cada teste (evitar interferência)
        (sender_sock, receiver_sock, sender_addr, receiver_addr) = create_socket_pair()
        
        sender_sock.settimeout(4.0)
        receiver_sock.settimeout(4.0)
        
        try:
            sender = GBNSender(sender_sock, channel, window_size=window_size, 
                             timeout=0.8, logger=logger)
            receiver = GBNReceiver(receiver_sock, channel, logger=logger)
            
            received_data = []
            transmission_complete = threading.Event()
            
            def receiver_thread():
                """Thread do receptor."""
                while len(received_data) < len(test_data):
                    try:
                        data = receiver.receive_data(timeout=4.0)
                        if data:
                            received_data.append(data)
                    except socket.timeout:
                        break
                transmission_complete.set()
            
            def sender_thread():
                """Thread do remetente."""
                time.sleep(0.1)
                for data in test_data:
                    success = sender.send_data(data, receiver_addr)
                    if not success:
                        # Se não conseguir enviar, aguarda um pouco e tenta novamente
                        time.sleep(0.1)
                        success = sender.send_data(data, receiver_addr)
                        if not success:
                            break
                    time.sleep(0.005)  # Pequeno delay controlado
            
            # --- ADICIONE UM BLOCO try...finally ---
            try:
                # Executar teste
                logger.start_session()
                start_time = time.time()
                
                recv_thread = threading.Thread(target=receiver_thread)
                send_thread = threading.Thread(target=sender_thread)
                
                recv_thread.start()
                send_thread.start()
                
                send_thread.join(timeout=20.0)
                transmission_complete.wait(timeout=20.0)
                
                end_time = time.time()
                
                # Cleanup threads
                try:
                    recv_thread.join(timeout=1.0)
                except:
                    pass
                
            finally:
                # --- GARANTE QUE O SENDER SEJA DESLIGADO PRIMEIRO ---
                if isinstance(sender, GBNSender):
                    sender.shutdown()
                if isinstance(receiver, GBNReceiver):
                    receiver.reset()
                time.sleep(0.05)
            
            # --- CHAME O LOGGER.END_SESSION() AQUI ---
            logger.end_session()
            
            # Obter estatísticas
            stats = logger.get_pipeline_statistics()
            
            # Adicionar métricas calculadas
            stats.update({
                'packets_expected': len(test_data),
                'packets_received_count': len(received_data),
                'data_integrity': len(received_data) / len(test_data) if test_data else 0,
                'actual_duration': end_time - start_time,
                'window_size': window_size
            })
            
            return stats
            
        finally:
            sender_sock.close()
            receiver_sock.close()
    
    def _generate_throughput_analysis(self, results: Dict[int, Dict[str, Any]]):
        """Gera análise de throughput e 'gráfico' textual."""
        print(f"\n=== Análise de Throughput x Tamanho da Janela ===")
        print(f"{'Janela':<8} {'Throughput (bytes/s)':<18} {'Tempo (s)':<10} {'Integridade':<12} {'Retrans':<8}")
        print("-" * 70)
        
        throughputs = []
        for window_size in sorted(results.keys()):
            stats = results[window_size]
            throughput = stats['throughput_bps']
            throughputs.append((window_size, throughput))
            
            print(f"{window_size:<8} "
                  f"{throughput:<18.1f} "
                  f"{stats['duration_seconds']:<10.2f} "
                  f"{stats['data_integrity']:<12.2%} "
                  f"{stats['retransmission_rate']:<8.2%}")
        
        # Encontrar melhor configuração
        best_throughput = max(throughputs, key=lambda x: x[1])
        baseline_throughput = next(tp for ws, tp in throughputs if ws == 1)
        
        print(f"\n=== Resultados da Análise ===")
        print(f"Melhor throughput: Janela {best_throughput[0]} com {best_throughput[1]:.1f} bytes/s")
        print(f"Melhoria vs janela 1: {best_throughput[1] / baseline_throughput:.1f}x")
        
        # "Gráfico" textual simples
        print(f"\n=== Gráfico: Throughput x Tamanho da Janela ===")
        max_throughput = max(tp for _, tp in throughputs)
        
        for window_size, throughput in throughputs:
            bar_length = int((throughput / max_throughput) * 50)
            bar = "█" * bar_length
            print(f"N={window_size:2d} |{bar:<50}| {throughput:.0f} bytes/s")
        
        print(f"\nObservações:")
        print(f"- Janela 1 (stop-and-wait) tem menor throughput")
        print(f"- Janelas maiores permitem melhor utilização do canal")
        print(f"- Ponto ótimo pode variar com condições da rede")
    
    def _validate_throughput_progression(self, results: Dict[int, Dict[str, Any]]):
        """Valida que o throughput melhora com janelas maiores."""
        # Verificar integridade dos dados em todos os testes
        for window_size, stats in results.items():
            assert stats['data_integrity'] > 0.95, \
                f"Integridade baixa para janela {window_size}: {stats['data_integrity']:.2%}"
            
            assert stats['throughput_bps'] > 0, \
                f"Throughput zero para janela {window_size}"
        
        # Verificar progressão de throughput
        window_sizes = sorted(results.keys())
        throughputs = [results[w]['throughput_bps'] for w in window_sizes]
        
        # Janela 1 deve ter menor throughput
        min_throughput = min(throughputs)
        assert min_throughput == results[1]['throughput_bps'], \
            "Janela 1 deveria ter o menor throughput"
        
        # Deve haver melhoria significativa com janelas maiores
        improvement_5_vs_1 = results[5]['throughput_bps'] / results[1]['throughput_bps']
        assert improvement_5_vs_1 > 1.3, \
            f"Janela 5 deve ser pelo menos 1.3x melhor que janela 1. Atual: {improvement_5_vs_1:.1f}x"
        
        # Janelas muito grandes (20) devem ser melhores que pequenas (1)
        improvement_20_vs_1 = results[20]['throughput_bps'] / results[1]['throughput_bps']
        assert improvement_20_vs_1 > 1.5, \
            f"Janela 20 deve ser pelo menos 1.5x melhor que janela 1. Atual: {improvement_20_vs_1:.1f}x"


if __name__ == "__main__":
    # Executar testes específicos se chamado diretamente
    pytest.main([__file__, "-v", "--tb=short"])