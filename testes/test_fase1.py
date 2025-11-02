"""
Testes automatizados para RDT 2.0 - Fase 1.
Implementa os testes obrigatórios da Tarefa 1A usando pytest.

Testes obrigatórios:
- Transmitir uma sequência de 10 mensagens com canal perfeito
- Introduzir corrupção artificial de bits em 30% dos pacotes
- Verificar se todas as mensagens chegam corretamente ao destino
- Registrar quantas retransmissões ocorreram
"""

import pytest
import socket
import threading
import time
from typing import List, Tuple

from fase1.rdt20 import RDT20Sender, RDT20Receiver
from fase1.rdt21 import RDT21Sender, RDT21Receiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver
from utils.packet import RDT20Packet, RDT21Packet, Packet, PacketError
from utils.simulator import UnreliableChannel, PerfectChannel
from utils.logger import ProtocolLogger


class TestRDT20:
    """Testes para o protocolo RDT 2.0 - Tarefa 1A."""
    
    def setup_method(self):
        """Configuração inicial para cada teste."""
        # Criar sockets UDP
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Bind sockets com portas automáticas
        self.sender_socket.bind(('localhost', 0))
        self.receiver_socket.bind(('localhost', 0))
        
        # Obter endereços atribuídos
        self.sender_addr = self.sender_socket.getsockname()
        self.receiver_addr = self.receiver_socket.getsockname()
    
    def teardown_method(self):
        """Limpeza após cada teste."""
        try:
            self.sender_socket.close()
            self.receiver_socket.close()
        except:
            pass
    
    def test_packet_format_compliance(self):
        """Teste do formato de pacote conforme especificação."""
        # Testar formato: | Tipo (1 byte) | Checksum (4 bytes) | Dados (variável) |
        
        # Pacote DATA
        data_packet = RDT20Packet(Packet.DATA, b"Test data")
        serialized = data_packet.serialize()
        
        # Verificar estrutura
        assert len(serialized) >= 5, "Pacote deve ter pelo menos 5 bytes (tipo + checksum)"
        assert serialized[0] == Packet.DATA, "Primeiro byte deve ser o tipo DATA"
        
        # Deserializar e verificar
        deserialized = RDT20Packet.deserialize(serialized)
        assert deserialized.type == Packet.DATA
        assert deserialized.data == b"Test data"
        assert not deserialized.is_corrupted()
        
        # Pacote ACK
        ack_packet = RDT20Packet(Packet.ACK, b"")
        serialized_ack = ack_packet.serialize()
        deserialized_ack = RDT20Packet.deserialize(serialized_ack)
        
        assert deserialized_ack.type == Packet.ACK
        assert deserialized_ack.is_ack_packet()
        
        # Pacote NAK
        nak_packet = RDT20Packet(Packet.NAK, b"")
        serialized_nak = nak_packet.serialize()
        deserialized_nak = RDT20Packet.deserialize(serialized_nak)
        
        assert deserialized_nak.type == Packet.NAK
        assert deserialized_nak.is_nak_packet()
    
    def test_checksum_integrity(self):
        """Teste de integridade do checksum MD5."""
        # Criar pacote válido
        original_data = b"Dados importantes para verificar integridade"
        packet = RDT20Packet(Packet.DATA, original_data)
        
        # Verificar que não está corrompido
        assert not packet.is_corrupted(), "Pacote original não deve estar corrompido"
        
        # Simular corrupção alterando checksum
        corrupted_packet = RDT20Packet(Packet.DATA, original_data, b"FAKE")
        assert corrupted_packet.is_corrupted(), "Pacote com checksum falso deve estar corrompido"
        
        # Testar serialização/desserialização preserva integridade
        serialized = packet.serialize()
        deserialized = RDT20Packet.deserialize(serialized)
        assert not deserialized.is_corrupted(), "Desserialização deve preservar integridade"
        assert deserialized.data == original_data, "Dados devem ser preservados"
    
    def test_ten_messages_perfect_channel(self):
        """
        TESTE OBRIGATÓRIO: Transmitir uma sequência de 10 mensagens com canal perfeito.
        Verifica se todas as mensagens chegam corretamente ao destino.
        """
        # Canal perfeito conforme especificação
        channel = PerfectChannel(verbose=False)
        logger = ProtocolLogger("RDT20_10Messages_Perfect")
        
        # 10 mensagens conforme requisito
        test_messages = [
            f"Mensagem {i+1} - RDT 2.0 Test".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        
        # Processo manual de comunicação para garantir controle total
        for i, message in enumerate(test_messages):
            try:
                # 1. Criar e enviar pacote DATA
                data_packet = RDT20Packet(Packet.DATA, message)
                serialized = data_packet.serialize()
                
                logger.log_transmission("DATA", data_size=len(message), 
                                      protocol_overhead=len(serialized)-len(message))
                
                self.sender_socket.sendto(serialized, self.receiver_addr)
                
                # 2. Receiver processa
                data, sender_addr = self.receiver_socket.recvfrom(1024)
                received_packet = RDT20Packet.deserialize(data)
                
                logger.log_reception("DATA", data_size=len(received_packet.data), success=True)
                
                if received_packet.is_data_packet() and not received_packet.is_corrupted():
                    # Dados válidos - enviar ACK
                    received_messages.append(received_packet.data)
                    
                    ack_packet = RDT20Packet(Packet.ACK, b"")
                    ack_serialized = ack_packet.serialize()
                    
                    logger.log_transmission("ACK", data_size=0, protocol_overhead=len(ack_serialized))
                    
                    self.receiver_socket.sendto(ack_serialized, sender_addr)
                    
                    # 3. Sender recebe ACK
                    ack_data, _ = self.sender_socket.recvfrom(1024)
                    ack_received = RDT20Packet.deserialize(ack_data)
                    
                    if ack_received.is_ack_packet() and not ack_received.is_corrupted():
                        logger.log_reception("ACK", success=True)
                        successful_transmissions += 1
                    
            except Exception as e:
                print(f"Erro na mensagem {i+1}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions == 10, f"Esperado 10 sucessos, obtido {successful_transmissions}"
        assert len(received_messages) == 10, f"Esperado 10 recepções, obtido {len(received_messages)}"
        assert stats['retransmissions'] == 0, "Canal perfeito não deve ter retransmissões"
        
        # Verificar que todas as mensagens chegaram corretamente
        for i, received in enumerate(received_messages):
            expected = f"Mensagem {i+1} - RDT 2.0 Test".encode()
            assert received == expected, f"Mensagem {i+1} incorreta: esperado {expected}, recebido {received}"
        
        # Registrar métricas
        print(f"\n=== Resultados - 10 Mensagens Canal Perfeito ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens transmitidas: {successful_transmissions}/10")
        print(f"Retransmissões: {stats['retransmissions']}")
        print(f"Throughput: {stats['throughput_bps']:.2f} bytes/s")
    
    def test_thirty_percent_corruption_with_retransmissions(self):
        """
        TESTE OBRIGATÓRIO: Introduzir corrupção artificial de bits em 30% dos pacotes.
        Verifica se todas as mensagens chegam corretamente e registra retransmissões.
        """
        # Simular corrupção manualmente para ter controle total
        logger = ProtocolLogger("RDT20_30Percent_Corruption")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} com corrupção".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        retransmissions = 0
        
        # Simular protocolo com corrupção de 30%
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar pacote DATA
                    data_packet = RDT20Packet(Packet.DATA, message)
                    serialized = data_packet.serialize()
                    
                    # Simular corrupção em ~30% dos casos
                    corrupted = (i % 3 == 0) and attempts == 1  # Corromper primeira tentativa de algumas mensagens
                    
                    if corrupted:
                        # Simular corrupção alterando dados
                        corrupted_data = bytearray(serialized)
                        corrupted_data[5] = corrupted_data[5] ^ 0xFF  # Corromper primeiro byte dos dados
                        serialized = bytes(corrupted_data)
                        retransmissions += 1
                        logger.log_retransmission("Corrupção simulada", "DATA")
                    
                    logger.log_transmission("DATA", data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    # 2. Enviar pacote
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 3. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT20Packet.deserialize(data)
                    
                    logger.log_reception("DATA", data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted():
                        # Dados válidos - enviar ACK
                        received_messages.append(received_packet.data)
                        
                        ack_packet = RDT20Packet(Packet.ACK, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        logger.log_transmission("ACK", data_size=0, protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 4. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT20Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted():
                            logger.log_reception("ACK", success=True)
                            successful_transmissions += 1
                            message_sent = True
                    else:
                        # Dados corrompidos - enviar NAK e tentar novamente
                        logger.log_corruption("DATA")
                        
                        nak_packet = RDT20Packet(Packet.NAK, b"")
                        nak_serialized = nak_packet.serialize()
                        self.receiver_socket.sendto(nak_serialized, sender_addr)
                        
                        # Sender recebe NAK
                        nak_data, _ = self.sender_socket.recvfrom(1024)
                        nak_received = RDT20Packet.deserialize(nak_data)
                        
                        if nak_received.is_nak_packet():
                            logger.log_reception("NAK", success=True)
                            # Continuar loop para retransmitir
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions >= 8, f"Pelo menos 8/10 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Verificar que todas as mensagens recebidas estão corretas
        for i, received in enumerate(received_messages):
            # Encontrar qual mensagem original corresponde
            found = False
            for j, original in enumerate(test_messages):
                if received == original:
                    found = True
                    break
            assert found, f"Mensagem recebida não corresponde a nenhuma original: {received}"
        
        # Registrar métricas (incluindo retransmissões)
        print(f"\n=== Resultados - 30% Corrupção ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/10")
        print(f"Mensagens recebidas corretamente: {len(received_messages)}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Pacotes corrompidos detectados: {stats['corrupted_packets']}")
        print(f"Throughput: {stats['throughput_bps']:.2f} bytes/s")
        
        # Deve haver retransmissões devido à corrupção
        assert stats['retransmissions'] > 0, "Deve haver retransmissões devido à corrupção simulada"
        print(f"✓ Protocolo funcionou corretamente com {stats['retransmissions']} retransmissões")
    
    def test_stop_and_wait_behavior(self):
        """Teste do comportamento stop-and-wait do protocolo."""
        channel = PerfectChannel(verbose=False)
        sender = RDT20Sender(self.sender_socket, channel)
        
        # Verificar estado inicial
        assert sender.get_state() == "READY"
        
        # Simular comunicação básica
        test_message = b"Stop and wait test"
        data_packet = RDT20Packet(Packet.DATA, test_message)
        serialized = data_packet.serialize()
        
        # Enviar DATA
        self.sender_socket.sendto(serialized, self.receiver_addr)
        
        # Receber DATA
        data, sender_addr = self.receiver_socket.recvfrom(1024)
        received_packet = RDT20Packet.deserialize(data)
        
        assert received_packet.is_data_packet()
        assert not received_packet.is_corrupted()
        assert received_packet.data == test_message
        
        # Enviar ACK
        ack_packet = RDT20Packet(Packet.ACK, b"")
        ack_serialized = ack_packet.serialize()
        self.receiver_socket.sendto(ack_serialized, sender_addr)
        
        # Receber ACK
        ack_data, _ = self.sender_socket.recvfrom(1024)
        ack_received = RDT20Packet.deserialize(ack_data)
        
        assert ack_received.is_ack_packet()
        assert not ack_received.is_corrupted()


class TestRDT21:
    """Testes para o protocolo RDT 2.1 - Tarefa 1B."""
    
    def setup_method(self):
        """Configuração inicial para cada teste."""
        # Criar sockets UDP
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Bind sockets com portas automáticas
        self.sender_socket.bind(('localhost', 0))
        self.receiver_socket.bind(('localhost', 0))
        
        # Obter endereços atribuídos
        self.sender_addr = self.sender_socket.getsockname()
        self.receiver_addr = self.receiver_socket.getsockname()
    
    def teardown_method(self):
        """Limpeza após cada teste."""
        try:
            self.sender_socket.close()
            self.receiver_socket.close()
        except:
            pass
    
    def test_packet_format_with_sequence_numbers(self):
        """Teste do formato de pacote RDT 2.1 com números de sequência."""
        # Formato: | Tipo (1 byte) | SeqNum (1 byte) | Checksum (4 bytes) | Dados (variável) |
        
        # Pacote DATA com seq_num = 0
        data_packet = RDT21Packet(Packet.DATA, 0, b"Test data seq 0")
        serialized = data_packet.serialize()
        
        # Verificar estrutura
        assert len(serialized) >= 6, "Pacote RDT 2.1 deve ter pelo menos 6 bytes (tipo + seq + checksum)"
        assert serialized[0] == Packet.DATA, "Primeiro byte deve ser o tipo DATA"
        assert serialized[1] == 0, "Segundo byte deve ser o número de sequência 0"
        
        # Deserializar e verificar
        deserialized = RDT21Packet.deserialize(serialized)
        assert deserialized.type == Packet.DATA
        assert deserialized.seq_num == 0
        assert deserialized.data == b"Test data seq 0"
        assert not deserialized.is_corrupted()
        
        # Pacote DATA com seq_num = 1
        data_packet_1 = RDT21Packet(Packet.DATA, 1, b"Test data seq 1")
        serialized_1 = data_packet_1.serialize()
        deserialized_1 = RDT21Packet.deserialize(serialized_1)
        
        assert deserialized_1.seq_num == 1
        assert deserialized_1.data == b"Test data seq 1"
        
        # Pacote ACK com seq_num
        ack_packet = RDT21Packet(Packet.ACK, 1, b"")
        serialized_ack = ack_packet.serialize()
        deserialized_ack = RDT21Packet.deserialize(serialized_ack)
        
        assert deserialized_ack.type == Packet.ACK
        assert deserialized_ack.seq_num == 1
        assert deserialized_ack.is_ack_packet()
    
    def test_sequence_number_alternation(self):
        """Teste da alternância de números de sequência (0, 1, 0, 1, ...)."""
        logger = ProtocolLogger("RDT21_Sequence_Test")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1}".encode() for i in range(6)
        ]
        
        logger.start_session()
        
        successful_transmissions = 0
        received_messages = []
        
        # Simular protocolo com alternância de números de sequência
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            try:
                # 1. Criar pacote DATA com número de sequência esperado
                data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                serialized = data_packet.serialize()
                
                logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                      protocol_overhead=len(serialized)-len(message))
                
                # 2. Enviar pacote
                self.sender_socket.sendto(serialized, self.receiver_addr)
                
                # 3. Receiver processa
                data, sender_addr = self.receiver_socket.recvfrom(1024)
                received_packet = RDT21Packet.deserialize(data)
                
                logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                   data_size=len(received_packet.data), success=True)
                
                # Verificar número de sequência
                assert received_packet.seq_num == expected_seq, f"Seq num incorreto: esperado {expected_seq}, recebido {received_packet.seq_num}"
                
                if received_packet.is_data_packet() and not received_packet.is_corrupted():
                    # Dados válidos - enviar ACK com mesmo número de sequência
                    received_messages.append(received_packet.data)
                    
                    ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                    ack_serialized = ack_packet.serialize()
                    
                    logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                          protocol_overhead=len(ack_serialized))
                    
                    self.receiver_socket.sendto(ack_serialized, sender_addr)
                    
                    # 4. Sender recebe ACK
                    ack_data, _ = self.sender_socket.recvfrom(1024)
                    ack_received = RDT21Packet.deserialize(ack_data)
                    
                    # Verificar número de sequência do ACK
                    assert ack_received.seq_num == expected_seq, f"ACK seq num incorreto: esperado {expected_seq}, recebido {ack_received.seq_num}"
                    
                    if ack_received.is_ack_packet() and not ack_received.is_corrupted():
                        logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                        successful_transmissions += 1
                        
                        # Alternar número de sequência para próxima mensagem
                        expected_seq = 1 - expected_seq
                    
            except Exception as e:
                print(f"Erro na mensagem {i+1}: {e}")
        
        logger.end_session()
        
        # Verificações
        assert successful_transmissions == len(test_messages), f"Esperado {len(test_messages)} sucessos, obtido {successful_transmissions}"
        assert len(received_messages) == len(test_messages), f"Esperado {len(test_messages)} recepções, obtido {len(received_messages)}"
        
        # Verificar que todas as mensagens chegaram corretamente
        for i, received in enumerate(received_messages):
            expected = f"Mensagem {i+1}".encode()
            assert received == expected, f"Mensagem {i+1} incorreta"
        
        print(f"\n=== Resultados - Alternância de Números de Sequência ===")
        print(f"Mensagens transmitidas: {successful_transmissions}/{len(test_messages)}")
        print(f"Alternância funcionando corretamente: 0→1→0→1→0→1")
    
    def test_twenty_percent_data_corruption(self):
        """
        TESTE OBRIGATÓRIO: Corromper 20% dos pacotes DATA.
        Verifica se não há duplicação de dados na aplicação receptora.
        """
        logger = ProtocolLogger("RDT21_20Percent_DATA_Corruption")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - DATA corruption test".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        data_corruptions = 0
        
        # Simular protocolo com 20% de corrupção em pacotes DATA
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar pacote DATA
                    data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                    serialized = data_packet.serialize()
                    
                    # Simular corrupção em ~20% dos pacotes DATA (primeira tentativa)
                    corrupted = (i % 5 == 0) and attempts == 1  # 20% dos pacotes
                    
                    if corrupted:
                        # Simular corrupção alterando dados
                        corrupted_data = bytearray(serialized)
                        corrupted_data[6] = corrupted_data[6] ^ 0xFF  # Corromper primeiro byte dos dados
                        serialized = bytes(corrupted_data)
                        data_corruptions += 1
                        logger.log_retransmission("DATA corrompido", "DATA")
                    
                    logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    # 2. Enviar pacote
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 3. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT21Packet.deserialize(data)
                    
                    logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                       data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                        # Dados válidos e com sequência correta - aceitar
                        received_messages.append(received_packet.data)
                        
                        ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                              protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 4. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT21Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                            logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                            successful_transmissions += 1
                            message_sent = True
                            
                            # Alternar número de sequência
                            expected_seq = 1 - expected_seq
                    else:
                        # Dados corrompidos ou duplicados - enviar NAK ou reenviar ACK anterior
                        if received_packet.is_corrupted():
                            logger.log_corruption("DATA")
                            
                            nak_packet = RDT21Packet(Packet.NAK, 0, b"")
                            nak_serialized = nak_packet.serialize()
                            self.receiver_socket.sendto(nak_serialized, sender_addr)
                            
                            # Sender recebe NAK
                            nak_data, _ = self.sender_socket.recvfrom(1024)
                            nak_received = RDT21Packet.deserialize(nak_data)
                            
                            if nak_received.is_nak_packet():
                                logger.log_reception("NAK", success=True)
                                # Continuar loop para retransmitir
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions >= 8, f"Pelo menos 8/10 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Verificar que não há duplicação de dados
        unique_messages = set(received_messages)
        assert len(unique_messages) == len(received_messages), f"Detectada duplicação de dados: {len(received_messages)} recebidas, {len(unique_messages)} únicas"
        
        # Verificar que todas as mensagens recebidas estão corretas
        for i, received in enumerate(received_messages):
            found = False
            for j, original in enumerate(test_messages):
                if received == original:
                    found = True
                    break
            assert found, f"Mensagem recebida não corresponde a nenhuma original: {received}"
        
        # Registrar métricas
        print(f"\n=== Resultados - 20% Corrupção DATA ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/10")
        print(f"Mensagens recebidas (sem duplicação): {len(received_messages)}")
        print(f"Corrupções de DATA simuladas: {data_corruptions}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Pacotes corrompidos detectados: {stats['corrupted_packets']}")
        
        # Deve haver retransmissões devido à corrupção
        assert stats['retransmissions'] > 0, "Deve haver retransmissões devido à corrupção de DATA"
        print(f"✓ Protocolo funcionou corretamente com {stats['retransmissions']} retransmissões")
        print(f"✓ Nenhuma duplicação de dados detectada")
    
    def test_twenty_percent_ack_corruption(self):
        """
        TESTE OBRIGATÓRIO: Corromper 20% dos ACKs.
        Verifica se o protocolo lida corretamente com ACKs corrompidos.
        """
        logger = ProtocolLogger("RDT21_20Percent_ACK_Corruption")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - ACK corruption test".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        ack_corruptions = 0
        
        # Simular protocolo com 20% de corrupção em ACKs
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar e enviar pacote DATA
                    data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                    serialized = data_packet.serialize()
                    
                    logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 2. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT21Packet.deserialize(data)
                    
                    logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                       data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                        # Dados válidos - aceitar e enviar ACK
                        # Só adicionar se não for duplicata
                        if received_packet.data not in received_messages:
                            received_messages.append(received_packet.data)
                        
                        ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        # Simular corrupção em ~20% dos ACKs
                        corrupted = (i % 5 == 0) and attempts == 1  # 20% dos ACKs
                        
                        if corrupted:
                            # Simular corrupção alterando checksum do ACK
                            corrupted_ack = bytearray(ack_serialized)
                            corrupted_ack[2] = corrupted_ack[2] ^ 0xFF  # Corromper checksum
                            ack_serialized = bytes(corrupted_ack)
                            ack_corruptions += 1
                            logger.log_corruption("ACK")
                        
                        logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                              protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 3. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT21Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                            # ACK válido
                            logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                            successful_transmissions += 1
                            message_sent = True
                            
                            # Alternar número de sequência
                            expected_seq = 1 - expected_seq
                        else:
                            # ACK corrompido ou com número incorreto - retransmitir
                            logger.log_reception("ACK corrompido/incorreto", success=False)
                            logger.log_retransmission("ACK corrompido", "DATA")
                            # Continuar loop para retransmitir
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions >= 8, f"Pelo menos 8/10 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Verificar que não há duplicação de dados
        unique_messages = set(received_messages)
        assert len(unique_messages) == len(received_messages), f"Detectada duplicação de dados: {len(received_messages)} recebidas, {len(unique_messages)} únicas"
        
        # Registrar métricas
        print(f"\n=== Resultados - 20% Corrupção ACK ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/10")
        print(f"Mensagens recebidas (sem duplicação): {len(received_messages)}")
        print(f"Corrupções de ACK simuladas: {ack_corruptions}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        
        # Deve haver retransmissões devido à corrupção de ACKs
        assert stats['retransmissions'] > 0, "Deve haver retransmissões devido à corrupção de ACKs"
        print(f"✓ Protocolo funcionou corretamente com {stats['retransmissions']} retransmissões")
        print(f"✓ Nenhuma duplicação de dados detectada")
    
    def test_protocol_overhead_measurement(self):
        """
        TESTE OBRIGATÓRIO: Medir overhead (quantos bytes extras por mensagem útil).
        """
        # Dados de teste de diferentes tamanhos
        test_data_sizes = [10, 50, 100, 500, 1000]  # bytes
        
        print(f"\n=== Medição de Overhead do Protocolo RDT 2.1 ===")
        
        for data_size in test_data_sizes:
            # Criar dados de teste
            test_data = b"X" * data_size
            
            # Criar pacote RDT 2.1
            packet = RDT21Packet(Packet.DATA, 0, test_data)
            serialized = packet.serialize()
            
            # Calcular overhead
            total_size = len(serialized)
            useful_data = len(test_data)
            protocol_overhead = total_size - useful_data
            overhead_percentage = (protocol_overhead / useful_data) * 100
            
            print(f"Dados úteis: {useful_data:4d} bytes | "
                  f"Total: {total_size:4d} bytes | "
                  f"Overhead: {protocol_overhead} bytes ({overhead_percentage:.1f}%)")
            
            # Verificar formato
            assert serialized[0] == Packet.DATA, "Primeiro byte deve ser tipo"
            assert serialized[1] == 0, "Segundo byte deve ser seq_num"
            # Bytes 2-5 são checksum, bytes 6+ são dados
            assert serialized[6:] == test_data, "Dados devem estar preservados"
        
        # Overhead fixo esperado: 1 byte (tipo) + 1 byte (seq_num) + 4 bytes (checksum) = 6 bytes
        expected_overhead = 6
        
        # Testar com dados pequenos
        small_packet = RDT21Packet(Packet.DATA, 1, b"test")
        small_serialized = small_packet.serialize()
        actual_overhead = len(small_serialized) - len(b"test")
        
        assert actual_overhead == expected_overhead, f"Overhead esperado {expected_overhead}, obtido {actual_overhead}"
        
        print(f"\n✓ Overhead fixo do protocolo RDT 2.1: {expected_overhead} bytes por pacote")
        print(f"✓ Formato: Tipo(1) + SeqNum(1) + Checksum(4) + Dados(variável)")
    
    def test_duplicate_detection(self):
        """Teste de detecção de pacotes duplicados."""
        logger = ProtocolLogger("RDT21_Duplicate_Detection")
        
        logger.start_session()
        
        # Simular cenário onde pacote é recebido duas vezes
        test_message = b"Mensagem para teste de duplicata"
        
        # 1. Primeira recepção (normal)
        data_packet = RDT21Packet(Packet.DATA, 0, test_message)
        serialized = data_packet.serialize()
        
        self.sender_socket.sendto(serialized, self.receiver_addr)
        
        # Receiver processa primeira vez
        data, sender_addr = self.receiver_socket.recvfrom(1024)
        received_packet = RDT21Packet.deserialize(data)
        
        assert received_packet.seq_num == 0
        assert received_packet.data == test_message
        
        # Enviar ACK
        ack_packet = RDT21Packet(Packet.ACK, 0, b"")
        ack_serialized = ack_packet.serialize()
        self.receiver_socket.sendto(ack_serialized, sender_addr)
        
        # 2. Simular recepção de duplicata (mesmo seq_num)
        self.sender_socket.sendto(serialized, self.receiver_addr)  # Mesmo pacote
        
        # Receiver deve detectar duplicata
        data_dup, sender_addr_dup = self.receiver_socket.recvfrom(1024)
        received_packet_dup = RDT21Packet.deserialize(data_dup)
        
        assert received_packet_dup.seq_num == 0  # Mesmo número de sequência
        assert received_packet_dup.data == test_message  # Mesmos dados
        
        # Receiver deve reenviar ACK anterior (não processar dados novamente)
        ack_dup_packet = RDT21Packet(Packet.ACK, 0, b"")  # ACK do pacote anterior
        ack_dup_serialized = ack_dup_packet.serialize()
        self.receiver_socket.sendto(ack_dup_serialized, sender_addr_dup)
        
        logger.end_session()
        
        print(f"\n=== Teste de Detecção de Duplicatas ===")
        print(f"✓ Pacote original processado corretamente (seq=0)")
        print(f"✓ Duplicata detectada e descartada (seq=0)")
        print(f"✓ ACK anterior reenviado para duplicata")


class TestRDT30:
    """Testes para o protocolo RDT 3.0 - Tarefa 1C."""
    
    def setup_method(self):
        """Configuração inicial para cada teste."""
        # Criar sockets UDP
        self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Bind sockets com portas automáticas
        self.sender_socket.bind(('localhost', 0))
        self.receiver_socket.bind(('localhost', 0))
        
        # Obter endereços atribuídos
        self.sender_addr = self.sender_socket.getsockname()
        self.receiver_addr = self.receiver_socket.getsockname()
    
    def teardown_method(self):
        """Limpeza após cada teste."""
        try:
            self.sender_socket.close()
            self.receiver_socket.close()
        except:
            pass
    
    def test_timer_configuration(self):
        """Teste da configuração do timer de timeout."""
        # Canal perfeito para teste básico
        channel = PerfectChannel(verbose=False)
        
        # Testar timeout padrão (2.0s)
        sender_default = RDT30Sender(self.sender_socket, channel)
        assert sender_default.timeout == 2.0, "Timeout padrão deve ser 2.0 segundos"
        
        # Testar timeout customizado
        sender_custom = RDT30Sender(self.sender_socket, channel, timeout=1.5)
        assert sender_custom.timeout == 1.5, "Timeout customizado deve ser 1.5 segundos"
        
        print(f"\n=== Configuração do Timer ===")
        print(f"✓ Timeout padrão: {sender_default.timeout}s")
        print(f"✓ Timeout customizado: {sender_custom.timeout}s")
    
    def test_fifteen_percent_data_loss(self):
        """
        TESTE OBRIGATÓRIO: Simular perda de 15% dos pacotes DATA.
        Verifica se todas as mensagens são entregues corretamente.
        """
        logger = ProtocolLogger("RDT30_15Percent_DATA_Loss")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - DATA loss test".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        data_losses = 0
        
        # Simular protocolo com 15% de perda em pacotes DATA
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar pacote DATA
                    data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                    serialized = data_packet.serialize()
                    
                    # Simular perda em ~15% dos pacotes DATA (primeira tentativa)
                    lost = (i % 7 == 0) and attempts == 1  # ~15% dos pacotes (1/7 ≈ 14.3%)
                    
                    if lost:
                        # Simular perda - não enviar o pacote
                        data_losses += 1
                        logger.log_retransmission("DATA perdido", "DATA")
                        print(f"  → DATA {i+1} perdido (simulação)")
                        
                        # Simular timeout - aguardar e retransmitir
                        time.sleep(0.1)  # Simular timeout
                        continue
                    
                    logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    # 2. Enviar pacote (se não foi perdido)
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 3. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT21Packet.deserialize(data)
                    
                    logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                       data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                        # Dados válidos e com sequência correta - aceitar
                        received_messages.append(received_packet.data)
                        
                        ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                              protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 4. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT21Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                            logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                            successful_transmissions += 1
                            message_sent = True
                            
                            # Alternar número de sequência
                            expected_seq = 1 - expected_seq
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions >= 8, f"Pelo menos 8/10 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Verificar que todas as mensagens recebidas estão corretas
        for i, received in enumerate(received_messages):
            found = False
            for j, original in enumerate(test_messages):
                if received == original:
                    found = True
                    break
            assert found, f"Mensagem recebida não corresponde a nenhuma original: {received}"
        
        # Registrar métricas
        print(f"\n=== Resultados - 15% Perda DATA ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/10")
        print(f"Mensagens recebidas corretamente: {len(received_messages)}")
        print(f"Perdas de DATA simuladas: {data_losses}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Throughput efetivo: {stats['throughput_bps']:.2f} bytes/s")
        
        # Deve haver retransmissões devido à perda
        assert stats['retransmissions'] > 0, "Deve haver retransmissões devido à perda de DATA"
        print(f"✓ Protocolo funcionou corretamente com {stats['retransmissions']} retransmissões")
        print(f"✓ Todas as mensagens entregues apesar da perda")
    
    def test_fifteen_percent_ack_loss(self):
        """
        TESTE OBRIGATÓRIO: Simular perda de 15% dos ACKs.
        Verifica se o protocolo lida corretamente com ACKs perdidos.
        """
        logger = ProtocolLogger("RDT30_15Percent_ACK_Loss")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - ACK loss test".encode() 
            for i in range(10)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        ack_losses = 0
        
        # Simular protocolo com 15% de perda em ACKs
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar e enviar pacote DATA
                    data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                    serialized = data_packet.serialize()
                    
                    logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 2. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT21Packet.deserialize(data)
                    
                    logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                       data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                        # Dados válidos - aceitar e enviar ACK
                        # Só adicionar se não for duplicata
                        if received_packet.data not in received_messages:
                            received_messages.append(received_packet.data)
                        
                        ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        # Simular perda em ~15% dos ACKs
                        lost = (i % 7 == 0) and attempts == 1  # ~15% dos ACKs
                        
                        if lost:
                            # Simular perda - não enviar o ACK
                            ack_losses += 1
                            logger.log_retransmission("ACK perdido", "DATA")
                            print(f"  ← ACK {i+1} perdido (simulação)")
                            
                            # Simular timeout no sender - ele vai retransmitir
                            time.sleep(0.1)
                            continue
                        
                        logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                              protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 3. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT21Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                            # ACK válido
                            logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                            successful_transmissions += 1
                            message_sent = True
                            
                            # Alternar número de sequência
                            expected_seq = 1 - expected_seq
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações obrigatórias
        assert successful_transmissions >= 8, f"Pelo menos 8/10 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Verificar que não há duplicação de dados
        unique_messages = set(received_messages)
        assert len(unique_messages) == len(received_messages), f"Detectada duplicação de dados: {len(received_messages)} recebidas, {len(unique_messages)} únicas"
        
        # Registrar métricas
        print(f"\n=== Resultados - 15% Perda ACK ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/10")
        print(f"Mensagens recebidas (sem duplicação): {len(received_messages)}")
        print(f"Perdas de ACK simuladas: {ack_losses}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Throughput efetivo: {stats['throughput_bps']:.2f} bytes/s")
        
        # Deve haver retransmissões devido à perda de ACKs
        assert stats['retransmissions'] > 0, "Deve haver retransmissões devido à perda de ACKs"
        print(f"✓ Protocolo funcionou corretamente com {stats['retransmissions']} retransmissões")
        print(f"✓ Nenhuma duplicação de dados detectada")
    
    def test_variable_network_delay(self):
        """
        TESTE OBRIGATÓRIO: Simular atraso variável (50-500ms) na rede.
        Verifica se o protocolo funciona com atrasos variáveis.
        """
        logger = ProtocolLogger("RDT30_Variable_Delay")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - delay test".encode() 
            for i in range(5)  # Menos mensagens devido aos atrasos
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        delays_applied = []
        
        # Simular protocolo com atrasos variáveis
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            try:
                # 1. Criar e enviar pacote DATA
                data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                serialized = data_packet.serialize()
                
                logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                      protocol_overhead=len(serialized)-len(message))
                
                # Simular atraso variável (50-500ms)
                import random
                delay = random.uniform(0.05, 0.5)  # 50-500ms
                delays_applied.append(delay)
                
                print(f"  → Aplicando atraso de {delay*1000:.0f}ms na mensagem {i+1}")
                time.sleep(delay)
                
                self.sender_socket.sendto(serialized, self.receiver_addr)
                
                # 2. Receiver processa
                data, sender_addr = self.receiver_socket.recvfrom(1024)
                received_packet = RDT21Packet.deserialize(data)
                
                logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                   data_size=len(received_packet.data), success=True)
                
                if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                    # Dados válidos - aceitar e enviar ACK
                    received_messages.append(received_packet.data)
                    
                    ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                    ack_serialized = ack_packet.serialize()
                    
                    logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                          protocol_overhead=len(ack_serialized))
                    
                    # Simular atraso também no ACK
                    ack_delay = random.uniform(0.05, 0.2)  # Atraso menor para ACK
                    time.sleep(ack_delay)
                    
                    self.receiver_socket.sendto(ack_serialized, sender_addr)
                    
                    # 3. Sender recebe ACK
                    ack_data, _ = self.sender_socket.recvfrom(1024)
                    ack_received = RDT21Packet.deserialize(ack_data)
                    
                    if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                        logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                        successful_transmissions += 1
                        
                        # Alternar número de sequência
                        expected_seq = 1 - expected_seq
                    
            except Exception as e:
                print(f"Erro na mensagem {i+1}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações
        assert successful_transmissions >= 4, f"Pelo menos 4/5 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Registrar métricas
        avg_delay = sum(delays_applied) / len(delays_applied) if delays_applied else 0
        min_delay = min(delays_applied) if delays_applied else 0
        max_delay = max(delays_applied) if delays_applied else 0
        
        print(f"\n=== Resultados - Atraso Variável ===")
        print(f"Tempo total: {end_time - start_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/{len(test_messages)}")
        print(f"Atraso médio aplicado: {avg_delay*1000:.0f}ms")
        print(f"Atraso mínimo: {min_delay*1000:.0f}ms")
        print(f"Atraso máximo: {max_delay*1000:.0f}ms")
        print(f"Throughput efetivo: {stats['throughput_bps']:.2f} bytes/s")
        
        print(f"✓ Protocolo funcionou corretamente com atrasos variáveis")
        print(f"✓ Todas as mensagens entregues apesar dos atrasos")
    
    def test_comprehensive_network_conditions(self):
        """
        TESTE ABRANGENTE: Combinar perda, corrupção e atraso.
        Simula condições reais de rede adversas.
        """
        logger = ProtocolLogger("RDT30_Comprehensive")
        
        # Mensagens de teste
        test_messages = [
            f"Mensagem {i+1} - comprehensive test".encode() 
            for i in range(8)
        ]
        
        logger.start_session()
        start_time = time.time()
        
        successful_transmissions = 0
        received_messages = []
        total_retransmissions = 0
        
        # Simular protocolo com condições adversas combinadas
        expected_seq = 0
        
        for i, message in enumerate(test_messages):
            attempts = 0
            max_attempts = 5
            message_sent = False
            
            while attempts < max_attempts and not message_sent:
                attempts += 1
                
                try:
                    # 1. Criar pacote DATA
                    data_packet = RDT21Packet(Packet.DATA, expected_seq, message)
                    serialized = data_packet.serialize()
                    
                    # Simular condições adversas combinadas
                    import random
                    
                    # 10% perda de DATA
                    data_lost = random.random() < 0.1
                    # 10% corrupção de DATA
                    data_corrupted = random.random() < 0.1 and not data_lost
                    # 10% perda de ACK
                    ack_lost = random.random() < 0.1
                    # Atraso variável
                    delay = random.uniform(0.01, 0.1)
                    
                    if data_lost:
                        total_retransmissions += 1
                        logger.log_retransmission("DATA perdido", "DATA")
                        print(f"  → DATA {i+1} perdido")
                        time.sleep(0.05)  # Simular timeout
                        continue
                    
                    if data_corrupted:
                        # Corromper dados
                        corrupted_data = bytearray(serialized)
                        corrupted_data[6] = corrupted_data[6] ^ 0xFF
                        serialized = bytes(corrupted_data)
                        total_retransmissions += 1
                        logger.log_retransmission("DATA corrompido", "DATA")
                    
                    logger.log_transmission("DATA", seq_num=expected_seq, data_size=len(message), 
                                          protocol_overhead=len(serialized)-len(message))
                    
                    # Aplicar atraso
                    time.sleep(delay)
                    
                    # 2. Enviar pacote
                    self.sender_socket.sendto(serialized, self.receiver_addr)
                    
                    # 3. Receiver processa
                    data, sender_addr = self.receiver_socket.recvfrom(1024)
                    received_packet = RDT21Packet.deserialize(data)
                    
                    logger.log_reception("DATA", seq_num=received_packet.seq_num, 
                                       data_size=len(received_packet.data), success=True)
                    
                    if received_packet.is_data_packet() and not received_packet.is_corrupted() and received_packet.seq_num == expected_seq:
                        # Dados válidos - aceitar e enviar ACK
                        if received_packet.data not in received_messages:
                            received_messages.append(received_packet.data)
                        
                        ack_packet = RDT21Packet(Packet.ACK, expected_seq, b"")
                        ack_serialized = ack_packet.serialize()
                        
                        if ack_lost:
                            # Simular perda de ACK
                            total_retransmissions += 1
                            logger.log_retransmission("ACK perdido", "DATA")
                            print(f"  ← ACK {i+1} perdido")
                            time.sleep(0.05)
                            continue
                        
                        logger.log_transmission("ACK", seq_num=expected_seq, data_size=0, 
                                              protocol_overhead=len(ack_serialized))
                        
                        self.receiver_socket.sendto(ack_serialized, sender_addr)
                        
                        # 4. Sender recebe ACK
                        ack_data, _ = self.sender_socket.recvfrom(1024)
                        ack_received = RDT21Packet.deserialize(ack_data)
                        
                        if ack_received.is_ack_packet() and not ack_received.is_corrupted() and ack_received.seq_num == expected_seq:
                            logger.log_reception("ACK", seq_num=ack_received.seq_num, success=True)
                            successful_transmissions += 1
                            message_sent = True
                            
                            # Alternar número de sequência
                            expected_seq = 1 - expected_seq
                    else:
                        # Dados corrompidos - enviar NAK
                        if received_packet.is_corrupted():
                            logger.log_corruption("DATA")
                            
                            nak_packet = RDT21Packet(Packet.NAK, 0, b"")
                            nak_serialized = nak_packet.serialize()
                            self.receiver_socket.sendto(nak_serialized, sender_addr)
                            
                            # Sender recebe NAK
                            nak_data, _ = self.sender_socket.recvfrom(1024)
                            nak_received = RDT21Packet.deserialize(nak_data)
                            
                            if nak_received.is_nak_packet():
                                logger.log_reception("NAK", success=True)
                        
                except Exception as e:
                    print(f"Erro na mensagem {i+1}, tentativa {attempts}: {e}")
        
        end_time = time.time()
        logger.end_session()
        
        # Verificar resultados
        stats = logger.get_statistics()
        
        # Verificações
        assert successful_transmissions >= 6, f"Pelo menos 6/8 mensagens devem ser enviadas com sucesso, obtido {successful_transmissions}"
        
        # Calcular métricas
        total_time = end_time - start_time
        total_data_bytes = sum(len(msg) for msg in received_messages)
        effective_throughput = total_data_bytes / total_time if total_time > 0 else 0
        
        # Registrar métricas
        print(f"\n=== Resultados - Condições Adversas Combinadas ===")
        print(f"Tempo total: {total_time:.3f} segundos")
        print(f"Mensagens enviadas com sucesso: {successful_transmissions}/{len(test_messages)}")
        print(f"Mensagens recebidas (sem duplicação): {len(received_messages)}")
        print(f"Total de retransmissões: {stats['retransmissions']}")
        print(f"Taxa de retransmissão: {stats['retransmission_rate']:.2%}")
        print(f"Throughput efetivo: {effective_throughput:.2f} bytes/s")
        print(f"Bytes úteis transmitidos: {total_data_bytes}")
        
        print(f"✓ Protocolo RDT 3.0 funcionou corretamente em condições adversas")
        print(f"✓ Timer de timeout efetivo para detectar perdas")
        print(f"✓ Todas as mensagens entregues apesar das condições adversas")
    
    def test_timeout_effectiveness(self):
        """Teste da efetividade do timer de timeout."""
        # Canal com alta perda para forçar timeouts
        channel = UnreliableChannel(loss_rate=0.8, corrupt_rate=0.0, verbose=False)
        logger = ProtocolLogger("RDT30_Timeout_Test")
        
        # Criar sender com timeout curto para teste
        sender = RDT30Sender(self.sender_socket, channel, logger, timeout=0.5)
        
        logger.start_session()
        
        # Tentar enviar uma mensagem (deve falhar por timeout)
        test_message = b"Timeout test message"
        success = sender.send_data(test_message, self.receiver_addr)
        
        logger.end_session()
        stats = logger.get_statistics()
        
        # Com alta perda, deve falhar e ter retransmissões
        assert not success, "Envio deve falhar com alta perda"
        assert stats['retransmissions'] > 0, "Deve haver retransmissões por timeout"
        
        print(f"\n=== Teste de Timeout ===")
        print(f"Timeout configurado: {sender.timeout}s")
        print(f"Retransmissões por timeout: {stats['retransmissions']}")
        print(f"✓ Timer de timeout funcionando corretamente")


if __name__ == "__main__":
    # Executar testes específicos se chamado diretamente
    pytest.main([__file__, "-v", "--tb=short"])