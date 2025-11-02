#!/usr/bin/env python3
"""
Servidor TCP Simplificado - Exemplo de uso do SimpleTCPSocket

Este arquivo demonstra como usar a classe SimpleTCPSocket do lado servidor
para aceitar conexões e transferir dados conforme requisitos 1.2, 2.1 e 2.3.

Funcionalidades demonstradas:
- Modo de escuta para conexões entrantes
- Aceitação de conexões (three-way handshake)
- Recebimento de dados
- Envio de respostas
- Encerramento controlado da conexão
"""

import sys
import time
import threading
import argparse
try:
    from .tcp_socket import SimpleTCPSocket, ConnectionTimeout
    from ..utils.logger import ProtocolLogger
except ImportError:
    # Para execução direta do arquivo
    from tcp_socket import SimpleTCPSocket, ConnectionTimeout
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from utils.logger import ProtocolLogger

# Configura logger para o servidor
server_logger = ProtocolLogger('TCPServer')


def handle_client(client_socket: SimpleTCPSocket, client_addr: tuple, verbose: bool = False):
    """
    Trata conexão de um cliente específico.
    
    Implementa requisitos:
    - 2.1: Envio de dados através da conexão
    - 2.3: Recebimento de dados ordenados
    
    Args:
        client_socket: Socket da conexão com o cliente
        client_addr: Endereço do cliente
        verbose: Se deve imprimir informações detalhadas
    """
    try:
        if verbose:
            print(f"[Cliente {client_addr}] Conexão estabelecida")
            print(f"[Cliente {client_addr}] Estado: {client_socket.get_connection_state()}")
        
        # Loop de comunicação com o cliente
        message_count = 0
        
        while client_socket.is_connected():
            try:
                # Requisito 2.3: Recebe dados do cliente (ordenados por sequência)
                data = client_socket.recv(1024)
                
                if not data:
                    # Cliente fechou conexão ou não há dados
                    if verbose:
                        print(f"[Cliente {client_addr}] Sem dados recebidos, verificando conexão...")
                    time.sleep(0.1)
                    continue
                
                message_count += 1
                message = data.decode('utf-8')
                
                if verbose:
                    print(f"[Cliente {client_addr}] Mensagem {message_count} recebida ({len(data)} bytes): '{message}'")
                else:
                    print(f"Cliente {client_addr}: {message}")
                
                # Requisito 2.1: Envia resposta ao cliente
                response = f"Servidor recebeu mensagem {message_count}: '{message}'"
                response_bytes = response.encode('utf-8')
                
                bytes_sent = client_socket.send(response_bytes)
                
                if verbose:
                    print(f"[Cliente {client_addr}] Resposta enviada ({bytes_sent} bytes)")
                
                # Verifica se cliente enviou comando de encerramento
                if message.lower().strip() in ['quit', 'exit', 'bye']:
                    if verbose:
                        print(f"[Cliente {client_addr}] Comando de encerramento recebido")
                    break
                
            except Exception as e:
                if verbose:
                    print(f"[Cliente {client_addr}] Erro na comunicação: {e}")
                break
        
        # Estatísticas da sessão
        if verbose:
            print(f"[Cliente {client_addr}] Sessão encerrada - {message_count} mensagens processadas")
            
            rtt_stats = client_socket.get_rtt_stats()
            print(f"[Cliente {client_addr}] RTT médio: {rtt_stats['estimated_rtt']:.3f}s")
            
            buffer_stats = client_socket.get_buffer_stats()
            print(f"[Cliente {client_addr}] Dados pendentes no buffer: {buffer_stats['send_buffer']['available_data']} bytes")
    
    except Exception as e:
        print(f"[Cliente {client_addr}] Erro inesperado: {e}")
    
    finally:
        # Encerra conexão de forma controlada
        if verbose:
            print(f"[Cliente {client_addr}] Encerrando conexão...")
        
        try:
            client_socket.close()
            if verbose:
                print(f"[Cliente {client_addr}] Conexão encerrada")
        except Exception as e:
            if verbose:
                print(f"[Cliente {client_addr}] Erro ao encerrar: {e}")


def run_server(port: int = 8080, verbose: bool = False, single_client: bool = False):
    """
    Executa servidor TCP simplificado.
    
    Implementa requisitos:
    - 1.2: Modo de escuta e aceitação de conexões do lado servidor
    - 2.1: Envio de dados através da conexão
    - 2.3: Recebimento de dados ordenados
    
    Args:
        port: Porta para escutar conexões
        verbose: Se deve imprimir informações detalhadas
        single_client: Se deve aceitar apenas um cliente por vez
    """
    server_socket = None
    client_threads = []
    
    try:
        # Cria socket TCP simplificado
        server_socket = SimpleTCPSocket(port=port)
        
        if verbose:
            local_addr = server_socket.get_local_address()
            print(f"Servidor TCP iniciado em {local_addr[0]}:{local_addr[1]}")
        else:
            print(f"Servidor TCP escutando na porta {port}")
        
        # Requisito 1.2: Coloca socket em modo de escuta
        server_socket.listen()
        
        if verbose:
            print(f"Estado do servidor: {server_socket.get_connection_state()}")
            print("Aguardando conexões de clientes...")
        
        print("Pressione Ctrl+C para parar o servidor")
        
        while True:
            try:
                # Requisito 1.2: Aceita conexão entrante (three-way handshake)
                if verbose:
                    print("\nAguardando nova conexão...")
                
                client_socket = server_socket.accept()
                client_addr = client_socket.get_peer_address()
                
                print(f"Nova conexão aceita de {client_addr}")
                
                if single_client:
                    # Modo single-client: trata cliente na thread principal
                    handle_client(client_socket, client_addr, verbose)
                else:
                    # Modo multi-client: cria thread para cada cliente
                    client_thread = threading.Thread(
                        target=handle_client,
                        args=(client_socket, client_addr, verbose),
                        daemon=True,
                        name=f"Client-{client_addr[0]}:{client_addr[1]}"
                    )
                    client_thread.start()
                    client_threads.append(client_thread)
                    
                    if verbose:
                        print(f"Thread criada para cliente {client_addr}")
                
            except ConnectionTimeout:
                if verbose:
                    print("Timeout aguardando conexão (normal)")
                continue
            except KeyboardInterrupt:
                print("\nParando servidor...")
                break
            except Exception as e:
                print(f"Erro ao aceitar conexão: {e}")
                continue
    
    except Exception as e:
        print(f"Erro ao iniciar servidor: {e}")
        return 1
    
    finally:
        # Encerra servidor e aguarda threads dos clientes
        if server_socket:
            if verbose:
                print("Encerrando servidor...")
            
            try:
                server_socket.close()
                if verbose:
                    print("Socket do servidor encerrado")
            except Exception as e:
                if verbose:
                    print(f"Erro ao encerrar servidor: {e}")
        
        # Aguarda threads dos clientes terminarem
        if client_threads:
            if verbose:
                print(f"Aguardando {len(client_threads)} threads de clientes...")
            
            for thread in client_threads:
                if thread.is_alive():
                    thread.join(timeout=2.0)
            
            if verbose:
                print("Todas as threads de clientes finalizadas")
        
        print("Servidor encerrado")
    
    return 0


def run_echo_server(port: int = 8080, verbose: bool = False):
    """
    Executa servidor echo simples que retorna exatamente o que recebe.
    
    Args:
        port: Porta para escutar conexões
        verbose: Se deve imprimir informações detalhadas
    """
    server_socket = None
    
    try:
        server_socket = SimpleTCPSocket(port=port)
        
        print(f"Servidor Echo TCP na porta {port}")
        server_socket.listen()
        
        while True:
            try:
                print("Aguardando conexão...")
                client_socket = server_socket.accept()
                client_addr = client_socket.get_peer_address()
                
                print(f"Cliente conectado: {client_addr}")
                
                # Loop echo simples
                while client_socket.is_connected():
                    data = client_socket.recv(1024)
                    
                    if not data:
                        time.sleep(0.1)
                        continue
                    
                    message = data.decode('utf-8')
                    print(f"Echo: {message}")
                    
                    # Retorna exatamente o que recebeu
                    client_socket.send(data)
                    
                    if message.lower().strip() in ['quit', 'exit']:
                        break
                
                print(f"Cliente {client_addr} desconectado")
                client_socket.close()
                
            except KeyboardInterrupt:
                print("\nParando servidor echo...")
                break
            except Exception as e:
                print(f"Erro: {e}")
                continue
    
    except Exception as e:
        print(f"Erro ao iniciar servidor echo: {e}")
        return 1
    
    finally:
        if server_socket:
            server_socket.close()
        print("Servidor echo encerrado")
    
    return 0


def run_performance_server(port: int = 8080, verbose: bool = False):
    """
    Executa servidor para testes de desempenho.
    Mede throughput e estatísticas de conexão.
    
    Args:
        port: Porta para escutar conexões
        verbose: Se deve imprimir informações detalhadas
    """
    server_socket = None
    
    try:
        server_socket = SimpleTCPSocket(port=port)
        
        print(f"Servidor de Performance TCP na porta {port}")
        server_socket.listen()
        
        while True:
            try:
                print("Aguardando conexão para teste de performance...")
                client_socket = server_socket.accept()
                client_addr = client_socket.get_peer_address()
                
                print(f"Cliente conectado para teste: {client_addr}")
                
                # Estatísticas da sessão
                start_time = time.time()
                bytes_received = 0
                bytes_sent = 0
                message_count = 0
                
                while client_socket.is_connected():
                    data = client_socket.recv(4096)  # Buffer maior para performance
                    
                    if not data:
                        time.sleep(0.01)  # Sleep menor para melhor throughput
                        continue
                    
                    bytes_received += len(data)
                    message_count += 1
                    
                    # Resposta simples para confirmar recebimento
                    response = f"ACK {message_count}".encode('utf-8')
                    sent = client_socket.send(response)
                    bytes_sent += sent
                    
                    # Verifica comando de fim de teste
                    if b'END_TEST' in data:
                        break
                
                # Calcula estatísticas
                end_time = time.time()
                duration = end_time - start_time
                
                if duration > 0:
                    throughput_rx = bytes_received / duration / 1024  # KB/s
                    throughput_tx = bytes_sent / duration / 1024     # KB/s
                    
                    print(f"\nEstatísticas da sessão com {client_addr}:")
                    print(f"  Duração: {duration:.2f}s")
                    print(f"  Mensagens: {message_count}")
                    print(f"  Bytes recebidos: {bytes_received}")
                    print(f"  Bytes enviados: {bytes_sent}")
                    print(f"  Throughput RX: {throughput_rx:.2f} KB/s")
                    print(f"  Throughput TX: {throughput_tx:.2f} KB/s")
                    
                    # Estatísticas RTT
                    rtt_stats = client_socket.get_rtt_stats()
                    print(f"  RTT estimado: {rtt_stats['estimated_rtt']:.3f}s")
                    print(f"  Desvio RTT: {rtt_stats['dev_rtt']:.3f}s")
                
                client_socket.close()
                print(f"Teste com {client_addr} concluído\n")
                
            except KeyboardInterrupt:
                print("\nParando servidor de performance...")
                break
            except Exception as e:
                print(f"Erro no teste: {e}")
                continue
    
    except Exception as e:
        print(f"Erro ao iniciar servidor de performance: {e}")
        return 1
    
    finally:
        if server_socket:
            server_socket.close()
        print("Servidor de performance encerrado")
    
    return 0


def main():
    """
    Função principal do servidor TCP.
    Permite execução com diferentes modos e parâmetros.
    """
    parser = argparse.ArgumentParser(
        description="Servidor TCP Simplificado - Demonstração do SimpleTCPSocket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python tcp_server.py                    # Servidor padrão na porta 8080
  python tcp_server.py -p 9000           # Servidor na porta 9000
  python tcp_server.py -v                # Modo verboso
  python tcp_server.py -s                # Modo single-client
  python tcp_server.py --echo             # Servidor echo simples
  python tcp_server.py --performance      # Servidor para testes de performance
        """
    )
    
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Porta para escutar conexões (padrão: 8080)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Modo verboso (mostra informações detalhadas)')
    parser.add_argument('-s', '--single-client', action='store_true',
                        help='Aceita apenas um cliente por vez')
    parser.add_argument('--echo', action='store_true',
                        help='Modo echo (retorna exatamente o que recebe)')
    parser.add_argument('--performance', action='store_true',
                        help='Modo performance (para testes de throughput)')
    
    args = parser.parse_args()
    
    try:
        if args.echo:
            return run_echo_server(args.port, args.verbose)
        elif args.performance:
            return run_performance_server(args.port, args.verbose)
        else:
            return run_server(args.port, args.verbose, args.single_client)
    
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário")
        return 1


if __name__ == '__main__':
    sys.exit(main())