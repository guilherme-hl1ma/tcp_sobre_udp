#!/usr/bin/env python3
"""
Cliente TCP Simplificado - Exemplo de uso do SimpleTCPSocket

Este arquivo demonstra como usar a classe SimpleTCPSocket do lado cliente
para estabelecer conexões e transferir dados conforme requisitos 1.1, 2.1 e 2.3.

Funcionalidades demonstradas:
- Estabelecimento de conexão (three-way handshake)
- Envio de dados
- Recebimento de dados
- Encerramento controlado da conexão
"""

import sys
import time
import argparse
try:
    from .tcp_socket import SimpleTCPSocket, ConnectionTimeout, ConnectionRefused
except ImportError:
    # Para execução direta do arquivo
    from tcp_socket import SimpleTCPSocket, ConnectionTimeout, ConnectionRefused


def run_client(server_host: str = 'localhost', server_port: int = 8080, 
               message: str = "Hello from TCP client!", verbose: bool = False):
    """
    Executa cliente TCP simplificado.
    
    Implementa requisitos:
    - 1.1: Estabelecimento de conexão do lado cliente
    - 2.1: Envio de dados através da conexão
    - 2.3: Recebimento de dados ordenados
    
    Args:
        server_host: Endereço do servidor
        server_port: Porta do servidor
        message: Mensagem a enviar
        verbose: Se deve imprimir informações detalhadas
    """
    client_socket = None
    
    try:
        # Cria socket TCP simplificado (porta automática para cliente)
        client_socket = SimpleTCPSocket(port=0)
        
        if verbose:
            local_addr = client_socket.get_local_address()
            print(f"Cliente iniciado na porta {local_addr[1]}")
            print(f"Conectando ao servidor {server_host}:{server_port}...")
        
        # Requisito 1.1: Estabelece conexão TCP (three-way handshake)
        start_time = time.time()
        client_socket.connect((server_host, server_port))
        connection_time = time.time() - start_time
        
        if verbose:
            print(f"Conexão estabelecida em {connection_time:.3f}s")
            print(f"Estado da conexão: {client_socket.get_connection_state()}")
            print(f"Endereço do peer: {client_socket.get_peer_address()}")
        
        # Requisito 2.1: Envia dados através da conexão TCP
        message_bytes = message.encode('utf-8')
        
        if verbose:
            print(f"\nEnviando mensagem ({len(message_bytes)} bytes): '{message}'")
        
        bytes_sent = client_socket.send(message_bytes)
        
        if verbose:
            print(f"Enviados {bytes_sent} bytes")
        
        # Aguarda um pouco para garantir que dados sejam processados
        time.sleep(0.1)
        
        # Requisito 2.3: Recebe resposta do servidor (dados ordenados)
        if verbose:
            print("\nAguardando resposta do servidor...")
        
        response = client_socket.recv(1024)
        
        if response:
            response_str = response.decode('utf-8')
            if verbose:
                print(f"Resposta recebida ({len(response)} bytes): '{response_str}'")
            else:
                print(f"Servidor respondeu: {response_str}")
        else:
            print("Nenhuma resposta recebida do servidor")
        
        # Demonstra estatísticas da conexão
        if verbose:
            print(f"\nEstatísticas da conexão:")
            rtt_stats = client_socket.get_rtt_stats()
            print(f"  RTT estimado: {rtt_stats['estimated_rtt']:.3f}s")
            print(f"  Desvio RTT: {rtt_stats['dev_rtt']:.3f}s")
            print(f"  Timeout interval: {rtt_stats['timeout_interval']:.3f}s")
            
            buffer_stats = client_socket.get_buffer_stats()
            print(f"  Buffer de envio: {buffer_stats['send_buffer']['available_data']} bytes pendentes")
            print(f"  Buffer de recepção: {buffer_stats['receive_buffer']['available_data']} bytes disponíveis")
        
        # Aguarda um pouco antes de fechar para demonstrar conexão estável
        if verbose:
            print("\nConexão estabelecida com sucesso. Aguardando antes de fechar...")
            time.sleep(1.0)
        
    except ConnectionTimeout as e:
        print(f"Erro: Timeout na conexão - {e}")
        return 1
    except ConnectionRefused as e:
        print(f"Erro: Conexão recusada - {e}")
        return 1
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return 1
    
    finally:
        # Encerra conexão de forma controlada
        if client_socket:
            if verbose:
                print(f"\nEncerrando conexão...")
                print(f"Estado antes do close: {client_socket.get_connection_state()}")
            
            client_socket.close()
            
            if verbose:
                print(f"Estado após close: {client_socket.get_connection_state()}")
                print("Cliente encerrado")
    
    return 0


def run_interactive_client(server_host: str = 'localhost', server_port: int = 8080):
    """
    Executa cliente interativo que permite enviar múltiplas mensagens.
    
    Args:
        server_host: Endereço do servidor
        server_port: Porta do servidor
    """
    client_socket = None
    
    try:
        # Estabelece conexão
        client_socket = SimpleTCPSocket(port=0)
        print(f"Conectando ao servidor {server_host}:{server_port}...")
        
        client_socket.connect((server_host, server_port))
        print("Conexão estabelecida!")
        print("Digite mensagens para enviar (ou 'quit' para sair):")
        
        while True:
            try:
                # Lê mensagem do usuário
                message = input("> ").strip()
                
                if message.lower() in ['quit', 'exit', 'q']:
                    break
                
                if not message:
                    continue
                
                # Envia mensagem
                message_bytes = message.encode('utf-8')
                bytes_sent = client_socket.send(message_bytes)
                print(f"Enviados {bytes_sent} bytes")
                
                # Aguarda resposta
                response = client_socket.recv(1024)
                if response:
                    response_str = response.decode('utf-8')
                    print(f"Servidor: {response_str}")
                else:
                    print("Nenhuma resposta recebida")
                
            except KeyboardInterrupt:
                print("\nInterrompido pelo usuário")
                break
            except Exception as e:
                print(f"Erro ao processar mensagem: {e}")
    
    except Exception as e:
        print(f"Erro na conexão: {e}")
        return 1
    
    finally:
        if client_socket:
            print("Encerrando conexão...")
            client_socket.close()
            print("Cliente encerrado")
    
    return 0


def main():
    """
    Função principal do cliente TCP.
    Permite execução com diferentes modos e parâmetros.
    """
    parser = argparse.ArgumentParser(
        description="Cliente TCP Simplificado - Demonstração do SimpleTCPSocket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python tcp_client.py                          # Conecta ao localhost:8080
  python tcp_client.py -H 192.168.1.100        # Conecta ao IP específico
  python tcp_client.py -p 9000                 # Conecta à porta 9000
  python tcp_client.py -m "Olá servidor!"      # Envia mensagem específica
  python tcp_client.py -i                      # Modo interativo
  python tcp_client.py -v                      # Modo verboso
        """
    )
    
    parser.add_argument('-H', '--host', default='localhost',
                        help='Endereço do servidor (padrão: localhost)')
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='Porta do servidor (padrão: 8080)')
    parser.add_argument('-m', '--message', default='Hello from TCP client!',
                        help='Mensagem a enviar (padrão: "Hello from TCP client!")')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Modo interativo (permite múltiplas mensagens)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Modo verboso (mostra informações detalhadas)')
    
    args = parser.parse_args()
    
    try:
        if args.interactive:
            return run_interactive_client(args.host, args.port)
        else:
            return run_client(args.host, args.port, args.message, args.verbose)
    
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário")
        return 1


if __name__ == '__main__':
    sys.exit(main())