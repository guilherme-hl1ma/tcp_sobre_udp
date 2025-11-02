# Projeto de Redes de Computadores - Protocolos de Comunicação

Este projeto implementa protocolos de comunicação confiável em três fases progressivas, desde protocolos básicos de transferência confiável até uma implementação completa de TCP simplificado sobre UDP.

## Estrutura do Projeto

```
projeto_redes/
├── fase1/                  # Protocolos RDT (Reliable Data Transfer)
│   ├── rdt20.py           # RDT 2.0 - Detecção de erros
│   ├── rdt21.py           # RDT 2.1 - Números de sequência
│   └── rdt30.py           # RDT 3.0 - Timeouts e retransmissão
├── fase2/                  # Protocolo Go-Back-N
│   └── gbn.py             # Implementação Go-Back-N
├── fase3/                  # TCP Simplificado sobre UDP
│   ├── tcp_socket.py      # Implementação TCP completa
│   └── README.md          # Documentação específica da Fase 3
├── testes/                 # Suíte de testes completa
│   ├── test_fase1.py      # Testes RDT (17 testes)
│   ├── test_fase2.py      # Testes Go-Back-N (3 testes)
│   └── test_fase3.py      # Testes TCP (6 testes)
├── utils/                  # Utilitários compartilhados
│   └── logger.py          # Sistema de logging de protocolos
└── README.MD              # Este arquivo
```

## Pré-requisitos

- Python 3.8 ou superior
- pytest (para execução dos testes)

### Instalação das Dependências

```bash
pip install pytest
```

## Execução dos Testes

### Executar Todos os Testes

```bash
# Executa todos os testes de todas as fases
python -m pytest -s

# Executa com saída detalhada
python -m pytest -v -s

# Executa com relatório de cobertura
python -m pytest --tb=short -s
```

### Executar Testes por Fase

#### Fase 1 - Protocolos RDT (17 testes)

```bash
# Todos os testes da Fase 1
python -m pytest testes/test_fase1.py -v -s

# Testes específicos por protocolo
python -m pytest testes/test_fase1.py::TestRDT20 -v -s    # RDT 2.0
python -m pytest testes/test_fase1.py::TestRDT21 -v -s    # RDT 2.1
python -m pytest testes/test_fase1.py::TestRDT30 -v -s    # RDT 3.0

# Testes individuais RDT 2.0 (5 testes)
python -m pytest testes/test_fase1.py::TestRDT20::test_packet_format_compliance -v -s
python -m pytest testes/test_fase1.py::TestRDT20::test_checksum_integrity -v -s
python -m pytest testes/test_fase1.py::TestRDT20::test_ten_messages_perfect_channel -v -s
python -m pytest testes/test_fase1.py::TestRDT20::test_thirty_percent_corruption_with_retransmissions -v -s
python -m pytest testes/test_fase1.py::TestRDT20::test_stop_and_wait_behavior -v -s

# Testes individuais RDT 2.1 (6 testes)
python -m pytest testes/test_fase1.py::TestRDT21::test_packet_format_with_sequence_numbers -v -s
python -m pytest testes/test_fase1.py::TestRDT21::test_sequence_number_alternation -v -s
python -m pytest testes/test_fase1.py::TestRDT21::test_twenty_percent_data_corruption -v -s
python -m pytest testes/test_fase1.py::TestRDT21::test_twenty_percent_ack_corruption -v -s
python -m pytest testes/test_fase1.py::TestRDT21::test_protocol_overhead_measurement -v -s
python -m pytest testes/test_fase1.py::TestRDT21::test_duplicate_detection -v -s

# Testes individuais RDT 3.0 (6 testes)
python -m pytest testes/test_fase1.py::TestRDT30::test_timer_configuration -v -s
python -m pytest testes/test_fase1.py::TestRDT30::test_fifteen_percent_data_loss -v -s
python -m pytest testes/test_fase1.py::TestRDT30::test_fifteen_percent_ack_loss -v -s
python -m pytest testes/test_fase1.py::TestRDT30::test_variable_network_delay -v -s
python -m pytest testes/test_fase1.py::TestRDT30::test_comprehensive_network_conditions -v -s
python -m pytest testes/test_fase1.py::TestRDT30::test_timeout_effectiveness -v -s
```

#### Fase 2 - Go-Back-N (3 testes)

```bash
# Todos os testes da Fase 2
python -m pytest testes/test_fase2.py -v -s

# Testes individuais (3 testes)
python -m pytest testes/test_fase2.py::TestGBNObrigatorios::test_efficiency_1mb_transfer -v -s
python -m pytest testes/test_fase2.py::TestGBNObrigatorios::test_ten_percent_loss_reliability -v -s
python -m pytest testes/test_fase2.py::TestGBNObrigatorios::test_window_size_analysis -v -s
```

#### Fase 3 - TCP Simplificado (6 testes)

```bash
# Todos os testes da Fase 3
python -m pytest testes/test_fase3.py -v -s

# Testes específicos por categoria
python -m pytest testes/test_fase3.py::TestTCPConnectionEstablishment -v -s
python -m pytest testes/test_fase3.py::TestTCPDataTransfer -v -s
python -m pytest testes/test_fase3.py::TestTCPFlowControl -v -s
python -m pytest testes/test_fase3.py::TestTCPRetransmission -v -s
python -m pytest testes/test_fase3.py::TestTCPConnectionClose -v -s
python -m pytest testes/test_fase3.py::TestTCPPerformance -v -s

# Testes individuais (6 testes)
python -m pytest testes/test_fase3.py::TestTCPConnectionEstablishment::test_three_way_handshake_connection_establishment -v -s
python -m pytest testes/test_fase3.py::TestTCPDataTransfer::test_10kb_data_transfer_integrity -v -s
python -m pytest testes/test_fase3.py::TestTCPFlowControl::test_flow_control_with_small_window -v -s
python -m pytest testes/test_fase3.py::TestTCPRetransmission::test_retransmission_with_packet_loss -v -s
python -m pytest testes/test_fase3.py::TestTCPConnectionClose::test_four_way_handshake_connection_close -v -s
python -m pytest testes/test_fase3.py::TestTCPPerformance::test_1mb_performance_metrics -v -s
```

### Executar Teste Individual

```bash
# Exemplo: teste específico de transferência de dados
python -m pytest testes/test_fase3.py::TestTCPDataTransfer::test_10kb_data_transfer_integrity -v -s
```

## Uso dos Protocolos

### Fase 1 - Protocolos RDT

```python
from fase1.rdt30 import RDT30Sender, RDT30Receiver

# Configuração do canal
sender = RDT30Sender(sender_port=5000, receiver_port=5001)
receiver = RDT30Receiver(receiver_port=5001, sender_port=5000)

# Envio de mensagem
sender.rdt_send("Olá, mundo!")
```

### Fase 2 - Go-Back-N

```python
from fase2.gbn import GBNSender, GBNReceiver

# Configuração
sender = GBNSender(sender_port=6000, receiver_port=6001, window_size=4)
receiver = GBNReceiver(receiver_port=6001, sender_port=6000)

# Transferência de dados
data = b"Dados para transferir"
sender.send_data(data)
```

### Fase 3 - TCP Simplificado

```python
from fase3.tcp_socket import SimpleTCPSocket

# Servidor
server = SimpleTCPSocket(port=9000)
server.listen()
conn = server.accept()
data = conn.recv(1024)
conn.close()

# Cliente
client = SimpleTCPSocket()
client.connect(('localhost', 9000))
client.send(b"Hello TCP!")
client.close()
```

## Funcionalidades Implementadas

### Fase 1 - Reliable Data Transfer (RDT)

- **RDT 2.0**: Detecção de erros com checksums
- **RDT 2.1**: Números de sequência para detectar duplicatas
- **RDT 3.0**: Timeouts e retransmissão automática

### Fase 2 - Go-Back-N

- Janela deslizante para múltiplos pacotes em trânsito
- Retransmissão seletiva em caso de perda
- Controle de fluxo baseado em janela

### Fase 3 - TCP Simplificado

- **Estabelecimento de Conexão**: Three-way handshake completo
- **Transferência de Dados**: Envio/recebimento confiável com ordenação
- **Controle de Fluxo**: Janelas de recepção e controle de congestionamento
- **Retransmissão**: Timeout adaptativo e fast retransmit
- **Encerramento**: Four-way handshake com estados TCP completos
- **Métricas**: Logging detalhado e estatísticas de desempenho

## Testes Obrigatórios

### Fase 1 (17 testes)

- Conformidade de formato de pacotes
- Integridade de checksums
- Comportamento stop-and-wait
- Detecção de duplicatas
- Efetividade de timeouts

### Fase 2 (3 testes)

- Eficiência em transferência de 1MB
- Confiabilidade com 10% de perda
- Análise de tamanho de janela

### Fase 3 (6 testes)

- Estabelecimento de conexão (three-way handshake)
- Transferência de dados (10KB com verificação de integridade)
- Controle de fluxo (janela reduzida)
- Retransmissão (20% de perda simulada)
- Encerramento de conexão (four-way handshake)
- Métricas de desempenho (1MB)

## Logs e Debugging

O sistema inclui logging detalhado para análise de desempenho:

```python
# Obter estatísticas do protocolo
stats = socket.get_protocol_statistics()
print(f"Pacotes enviados: {stats['packets_sent']}")
print(f"Retransmissões: {stats['retransmissions']}")

# Gerar relatório detalhado
report = socket.generate_protocol_report(detailed=True)
print(report)
```

## Resolução de Problemas

### Erro "WinError 10054"

Este erro foi corrigido nas versões recentes. Se ainda ocorrer:

- Verifique se as portas não estão em uso
- Execute os testes individualmente
- Aguarde alguns segundos entre execuções

### Timeouts em Testes

Se os testes estão falhando por timeout:

- Verifique se não há firewall bloqueando
- Execute com `-v` para ver progresso detalhado
- Considere aumentar timeouts em ambientes lentos

### Performance

Para melhor performance nos testes:

- Execute testes individualmente para análise detalhada
- Use `--tb=short` para saída mais limpa
- Monitore uso de CPU/memória em testes de 1MB

## Contribuição

Para contribuir com o projeto:

1. Execute todos os testes antes de submeter alterações
2. Mantenha compatibilidade com as interfaces existentes
3. Adicione testes para novas funcionalidades
4. Documente mudanças significativas

## Status dos Testes

- ✅ **Fase 1**: 17/17 testes passando
- ✅ **Fase 2**: 3/3 testes passando
- ✅ **Fase 3**: 6/6 testes passando

**Total**: 26/26 testes passando (100% de sucesso)
