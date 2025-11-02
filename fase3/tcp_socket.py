import struct
import socket
import threading
import time
import random
from typing import Optional, Tuple

# Importa o sistema de logging do projeto
try:
    from ..utils.logger import ProtocolLogger
except ImportError:
    # Para execução direta do arquivo
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from utils.logger import ProtocolLogger

# Exceções TCP
class TCPError(Exception):
    """Exceção base para erros TCP."""
    pass

class ConnectionRefused(TCPError):
    """Exceção para conexão recusada."""
    pass

class ConnectionTimeout(TCPError):
    """Exceção para timeout de conexão."""
    pass

class ConnectionReset(TCPError):
    """Exceção para reset de conexão."""
    pass

# TCP Flags Constants
TCP_FIN = 0x01
TCP_SYN = 0x02
TCP_RST = 0x04
TCP_PSH = 0x08
TCP_ACK = 0x10
TCP_URG = 0x20

class TCPSegment:
    """
    Representa um segmento TCP com todos os campos do cabeçalho.
    Implementa serialização e deserialização para transmissão sobre UDP.
    """
    
    def __init__(self):
        self.source_port: int = 0      # 2 bytes
        self.dest_port: int = 0        # 2 bytes
        self.seq_num: int = 0          # 4 bytes
        self.ack_num: int = 0          # 4 bytes
        self.header_len: int = 20      # 1 byte (em palavras de 4 bytes, mínimo 5)
        self.flags: int = 0            # 1 byte (SYN, ACK, FIN, etc.)
        self.window_size: int = 0      # 2 bytes
        self.checksum: int = 0         # 2 bytes
        self.urgent_ptr: int = 0       # 2 bytes
        self.data: bytes = b''         # payload variável
    
    def pack(self) -> bytes:
        """
        Serializa o segmento TCP em bytes para transmissão.
        
        Returns:
            bytes: Segmento TCP serializado
        """
        # Header length em palavras de 4 bytes (mínimo 5 para 20 bytes)
        header_len_words = self.header_len // 4
        
        # Empacota o cabeçalho TCP usando struct
        # Formato: >HHLLBBHHH (big-endian)
        # H = unsigned short (2 bytes)
        # L = unsigned long (4 bytes) 
        # B = unsigned char (1 byte)
        header = struct.pack(
            '>HHLLBBHHH',
            self.source_port,
            self.dest_port,
            self.seq_num,
            self.ack_num,
            (header_len_words << 4),  # Header length nos 4 bits superiores
            self.flags,
            self.window_size,
            self.checksum,
            self.urgent_ptr
        )
        
        # Adiciona padding se necessário para completar 20 bytes mínimos
        if len(header) < 20:
            header += b'\x00' * (20 - len(header))
        
        # Concatena dados se existirem
        return header + self.data
    
    def unpack(self, data: bytes) -> None:
        """
        Deserializa bytes recebidos em um segmento TCP.
        
        Args:
            data: Bytes recebidos contendo segmento TCP
            
        Raises:
            struct.error: Se os dados não têm tamanho suficiente
        """
        if len(data) < 20:
            raise struct.error("Segmento TCP deve ter pelo menos 20 bytes")
        
        # Desempacota o cabeçalho TCP
        header_data = struct.unpack('>HHLLBBHHH', data[:20])
        
        self.source_port = header_data[0]
        self.dest_port = header_data[1]
        self.seq_num = header_data[2]
        self.ack_num = header_data[3]
        
        # Extrai header length dos 4 bits superiores
        header_len_words = (header_data[4] >> 4) & 0x0F
        self.header_len = header_len_words * 4
        
        self.flags = header_data[5]
        self.window_size = header_data[6]
        self.checksum = header_data[7]
        self.urgent_ptr = header_data[8]
        
        # Extrai dados se existirem (após o cabeçalho)
        if len(data) > self.header_len:
            self.data = data[self.header_len:]
        else:
            self.data = b''
    
    def has_flag(self, flag: int) -> bool:
        """
        Verifica se uma flag específica está definida.
        
        Args:
            flag: Flag TCP a verificar (TCP_SYN, TCP_ACK, etc.)
            
        Returns:
            bool: True se a flag estiver definida
        """
        return (self.flags & flag) != 0
    
    def set_flag(self, flag: int) -> None:
        """
        Define uma flag específica.
        
        Args:
            flag: Flag TCP a definir (TCP_SYN, TCP_ACK, etc.)
        """
        self.flags |= flag
    
    def clear_flag(self, flag: int) -> None:
        """
        Limpa uma flag específica.
        
        Args:
            flag: Flag TCP a limpar (TCP_SYN, TCP_ACK, etc.)
        """
        self.flags &= ~flag
    
    def __str__(self) -> str:
        """
        Representação string do segmento para debug.
        """
        flags_str = []
        if self.has_flag(TCP_SYN):
            flags_str.append("SYN")
        if self.has_flag(TCP_ACK):
            flags_str.append("ACK")
        if self.has_flag(TCP_FIN):
            flags_str.append("FIN")
        if self.has_flag(TCP_RST):
            flags_str.append("RST")
        if self.has_flag(TCP_PSH):
            flags_str.append("PSH")
        if self.has_flag(TCP_URG):
            flags_str.append("URG")
        
        return (f"TCPSegment(src={self.source_port}, dst={self.dest_port}, "
                f"seq={self.seq_num}, ack={self.ack_num}, "
                f"flags=[{','.join(flags_str)}], win={self.window_size}, "
                f"data_len={len(self.data)})")


class RTTManager:
    """
    Gerenciador de RTT (Round Trip Time) com estimativas adaptativas.
    Implementa cálculo de timeout baseado em EstimatedRTT e DevRTT conforme RFC 6298.
    """
    
    def __init__(self):
        """
        Inicializa o gerenciador de RTT com valores padrão.
        EstimatedRTT = 1.0s e DevRTT = 0.5s conforme especificação.
        """
        self.estimated_rtt: float = 1.0  # Estimativa de RTT em segundos
        self.dev_rtt: float = 0.5        # Desvio do RTT em segundos
        self.alpha: float = 0.875        # Fator de suavização para EstimatedRTT
        self.beta: float = 0.75          # Fator de suavização para DevRTT
        self.lock = threading.Lock()     # Thread safety
    
    def update_rtt(self, sample_rtt: float) -> None:
        """
        Atualiza as estimativas de RTT com uma nova amostra.
        
        Usa as fórmulas especificadas:
        - EstimatedRTT = 0.875 * EstimatedRTT + 0.125 * SampleRTT
        - DevRTT = 0.75 * DevRTT + 0.25 * |SampleRTT - EstimatedRTT|
        
        Args:
            sample_rtt: Nova amostra de RTT em segundos
        """
        if sample_rtt <= 0:
            return  # Ignora amostras inválidas
            
        with self.lock:
            # Calcula o erro absoluto antes de atualizar EstimatedRTT
            rtt_error = abs(sample_rtt - self.estimated_rtt)
            
            # Atualiza EstimatedRTT usando média móvel exponencial
            self.estimated_rtt = self.alpha * self.estimated_rtt + (1 - self.alpha) * sample_rtt
            
            # Atualiza DevRTT usando média móvel exponencial do erro
            self.dev_rtt = self.beta * self.dev_rtt + (1 - self.beta) * rtt_error
    
    def get_timeout_interval(self) -> float:
        """
        Calcula o intervalo de timeout baseado nas estimativas atuais.
        
        Usa a fórmula: TimeoutInterval = EstimatedRTT + 4 * DevRTT
        
        Returns:
            float: Intervalo de timeout em segundos
        """
        with self.lock:
            timeout = self.estimated_rtt + 4 * self.dev_rtt
            
            # Garante um timeout mínimo de 100ms para evitar timeouts muito agressivos
            min_timeout = 0.1
            
            # Garante um timeout máximo de 60s para evitar esperas excessivas
            max_timeout = 60.0
            
            return max(min_timeout, min(timeout, max_timeout))
    
    def get_estimated_rtt(self) -> float:
        """
        Retorna a estimativa atual de RTT.
        
        Returns:
            float: EstimatedRTT em segundos
        """
        with self.lock:
            return self.estimated_rtt
    
    def get_dev_rtt(self) -> float:
        """
        Retorna o desvio atual de RTT.
        
        Returns:
            float: DevRTT em segundos
        """
        with self.lock:
            return self.dev_rtt
    
    def reset(self) -> None:
        """
        Reseta as estimativas para os valores iniciais.
        Útil quando uma conexão é reiniciada.
        """
        with self.lock:
            self.estimated_rtt = 1.0
            self.dev_rtt = 0.5
    
    def __str__(self) -> str:
        """
        Representação string do RTTManager para debug.
        """
        with self.lock:
            return (f"RTTManager(estimated_rtt={self.estimated_rtt:.3f}s, "
                    f"dev_rtt={self.dev_rtt:.3f}s, "
                    f"timeout_interval={self.get_timeout_interval():.3f}s)")


class BufferManager:
    """
    Gerenciador de buffer circular para armazenamento de dados de envio e recepção.
    Implementa controle de posições de leitura e escrita com operações thread-safe.
    """
    
    def __init__(self, size: int):
        """
        Inicializa o buffer circular com tamanho especificado.
        
        Args:
            size: Tamanho do buffer em bytes
        """
        self.buffer: bytearray = bytearray(size)
        self.read_pos: int = 0
        self.write_pos: int = 0
        self.size: int = size
        self.lock = threading.Lock()
        self._data_available: int = 0  # Quantidade de dados disponíveis para leitura
    
    def write(self, data: bytes) -> int:
        """
        Escreve dados no buffer circular.
        
        Args:
            data: Dados a serem escritos
            
        Returns:
            int: Número de bytes efetivamente escritos
        """
        if not data:
            return 0
            
        with self.lock:
            available_space = self.available_space()
            
            # Limita a quantidade de dados ao espaço disponível
            bytes_to_write = min(len(data), available_space)
            
            if bytes_to_write == 0:
                return 0
            
            # Escreve os dados, tratando o wrap-around do buffer circular
            bytes_written = 0
            
            for byte in data[:bytes_to_write]:
                self.buffer[self.write_pos] = byte
                self.write_pos = (self.write_pos + 1) % self.size
                bytes_written += 1
            
            self._data_available += bytes_written
            return bytes_written
    
    def read(self, length: int) -> bytes:
        """
        Lê dados do buffer circular.
        
        Args:
            length: Número máximo de bytes a ler
            
        Returns:
            bytes: Dados lidos do buffer
        """
        if length <= 0:
            return b''
            
        with self.lock:
            available_data = self.available_data()
            
            # Limita a leitura aos dados disponíveis
            bytes_to_read = min(length, available_data)
            
            if bytes_to_read == 0:
                return b''
            
            # Lê os dados, tratando o wrap-around do buffer circular
            result = bytearray()
            
            for _ in range(bytes_to_read):
                result.append(self.buffer[self.read_pos])
                self.read_pos = (self.read_pos + 1) % self.size
            
            self._data_available -= bytes_to_read
            return bytes(result)
    
    def available_space(self) -> int:
        """
        Calcula o espaço disponível para escrita no buffer.
        
        Returns:
            int: Número de bytes disponíveis para escrita
        """
        # Espaço disponível = tamanho total - dados disponíveis
        return self.size - self._data_available
    
    def available_data(self) -> int:
        """
        Calcula a quantidade de dados disponíveis para leitura.
        
        Returns:
            int: Número de bytes disponíveis para leitura
        """
        return self._data_available
    
    def peek(self, length: int) -> bytes:
        """
        Visualiza dados sem removê-los do buffer (útil para reordenação).
        
        Args:
            length: Número máximo de bytes a visualizar
            
        Returns:
            bytes: Dados visualizados sem remoção
        """
        if length <= 0:
            return b''
            
        with self.lock:
            available_data = self.available_data()
            bytes_to_peek = min(length, available_data)
            
            if bytes_to_peek == 0:
                return b''
            
            # Visualiza os dados sem alterar read_pos
            result = bytearray()
            temp_pos = self.read_pos
            
            for _ in range(bytes_to_peek):
                result.append(self.buffer[temp_pos])
                temp_pos = (temp_pos + 1) % self.size
            
            return bytes(result)
    
    def clear(self) -> None:
        """
        Limpa todo o conteúdo do buffer.
        """
        with self.lock:
            self.read_pos = 0
            self.write_pos = 0
            self._data_available = 0
            # Não precisa limpar o buffer array, apenas resetar as posições
    
    def is_empty(self) -> bool:
        """
        Verifica se o buffer está vazio.
        
        Returns:
            bool: True se o buffer estiver vazio
        """
        return self._data_available == 0
    
    def is_full(self) -> bool:
        """
        Verifica se o buffer está cheio.
        
        Returns:
            bool: True se o buffer estiver cheio
        """
        return self._data_available == self.size
    
    def __str__(self) -> str:
        """
        Representação string do buffer para debug.
        """
        return (f"BufferManager(size={self.size}, available_data={self.available_data()}, "
                f"available_space={self.available_space()}, read_pos={self.read_pos}, "
                f"write_pos={self.write_pos})")


class ConnectionManager:
    """
    Gerenciador de conexão TCP com máquina de estados completa.
    Implementa transições entre estados e processamento de segmentos de controle.
    """
    
    # Estados da conexão TCP
    CLOSED = 'CLOSED'
    LISTEN = 'LISTEN'
    SYN_SENT = 'SYN_SENT'
    SYN_RCVD = 'SYN_RCVD'
    ESTABLISHED = 'ESTABLISHED'
    FIN_WAIT_1 = 'FIN_WAIT_1'
    FIN_WAIT_2 = 'FIN_WAIT_2'
    CLOSE_WAIT = 'CLOSE_WAIT'
    CLOSING = 'CLOSING'
    LAST_ACK = 'LAST_ACK'
    TIME_WAIT = 'TIME_WAIT'
    
    # Estados válidos
    VALID_STATES = {
        CLOSED, LISTEN, SYN_SENT, SYN_RCVD, ESTABLISHED,
        FIN_WAIT_1, FIN_WAIT_2, CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT
    }
    
    def __init__(self, socket_ref):
        """
        Inicializa o gerenciador de conexão.
        
        Args:
            socket_ref: Referência para o SimpleTCPSocket que possui este gerenciador
        """
        self.socket_ref = socket_ref
        self.state: str = self.CLOSED
        self.lock = threading.Lock()
        
        # Números de sequência e acknowledgment
        self.seq_num: int = 0
        self.ack_num: int = 0
        self.peer_seq_num: int = 0
        self.peer_ack_num: int = 0
        
        # Endereço do peer conectado
        self.peer_address: Optional[Tuple[str, int]] = None
        
        # Flags de controle
        self.connection_established = threading.Event()
        self.connection_closed = threading.Event()
        
        # Timer para TIME_WAIT
        self.time_wait_timer: Optional[threading.Timer] = None
        
        # Callback para notificar mudanças de estado (opcional)
        self.state_change_callback: Optional[callable] = None
    
    def get_state(self) -> str:
        """
        Retorna o estado atual da conexão de forma thread-safe.
        
        Returns:
            str: Estado atual da conexão
        """
        with self.lock:
            return self.state
    
    def set_state(self, new_state: str) -> None:
        """
        Altera o estado da conexão de forma thread-safe.
        
        Args:
            new_state: Novo estado da conexão
            
        Raises:
            ValueError: Se o estado for inválido
        """
        if new_state not in self.VALID_STATES:
            raise ValueError(f"Estado inválido: {new_state}")
        
        with self.lock:
            old_state = self.state
            self.state = new_state
            
            # Notifica eventos especiais
            if new_state == self.ESTABLISHED:
                self.connection_established.set()
            elif new_state == self.CLOSED:
                self.connection_closed.set()
                self.connection_established.clear()
            
            # Chama callback se definido
            if self.state_change_callback:
                try:
                    self.state_change_callback(old_state, new_state)
                except Exception:
                    pass  # Ignora erros no callback
    
    def is_connected(self) -> bool:
        """
        Verifica se a conexão está estabelecida.
        
        Returns:
            bool: True se no estado ESTABLISHED
        """
        return self.get_state() == self.ESTABLISHED
    
    def is_closed(self) -> bool:
        """
        Verifica se a conexão está fechada.
        
        Returns:
            bool: True se no estado CLOSED
        """
        return self.get_state() == self.CLOSED
    
    def can_send_data(self) -> bool:
        """
        Verifica se é possível enviar dados no estado atual.
        
        Returns:
            bool: True se pode enviar dados
        """
        state = self.get_state()
        return state in {self.ESTABLISHED, self.CLOSE_WAIT}
    
    def can_receive_data(self) -> bool:
        """
        Verifica se é possível receber dados no estado atual.
        
        Returns:
            bool: True se pode receber dados
        """
        state = self.get_state()
        return state in {self.ESTABLISHED, self.FIN_WAIT_1, self.FIN_WAIT_2}
    
    def send_syn(self, dest_addr: Tuple[str, int]) -> None:
        """
        Envia segmento SYN para estabelecer conexão.
        
        Args:
            dest_addr: Endereço de destino (host, porta)
        """
        segment = TCPSegment()
        segment.source_port = self.socket_ref.local_port
        segment.dest_port = dest_addr[1]
        segment.seq_num = self.seq_num
        segment.ack_num = 0
        segment.set_flag(TCP_SYN)
        segment.window_size = self.socket_ref._calculate_receive_window()
        
        self._send_segment(segment, dest_addr)
        
        # Atualiza estado e números de sequência
        self.set_state(self.SYN_SENT)
        self.peer_address = dest_addr
        self.seq_num += 1  # SYN consome um número de sequência
    
    def send_syn_ack(self, dest_addr: Tuple[str, int]) -> None:
        """
        Envia segmento SYN-ACK em resposta a um SYN.
        
        Args:
            dest_addr: Endereço de destino (host, porta)
        """
        segment = TCPSegment()
        segment.source_port = self.socket_ref.local_port
        segment.dest_port = dest_addr[1]
        segment.seq_num = self.seq_num
        segment.ack_num = self.ack_num
        segment.set_flag(TCP_SYN | TCP_ACK)
        segment.window_size = self.socket_ref._calculate_receive_window()
        
        self._send_segment(segment, dest_addr)
        
        # Atualiza estado e números de sequência
        self.set_state(self.SYN_RCVD)
        self.peer_address = dest_addr
        self.seq_num += 1  # SYN consome um número de sequência
    
    def send_ack(self, dest_addr: Tuple[str, int]) -> None:
        """
        Envia segmento ACK puro (sem dados).
        
        Args:
            dest_addr: Endereço de destino (host, porta)
        """
        segment = TCPSegment()
        segment.source_port = self.socket_ref.local_port
        segment.dest_port = dest_addr[1]
        segment.seq_num = self.seq_num
        segment.ack_num = self.ack_num
        segment.set_flag(TCP_ACK)
        segment.window_size = self.socket_ref._calculate_receive_window()
        
        self._send_segment(segment, dest_addr)
    
    def send_fin(self, dest_addr: Tuple[str, int]) -> None:
        """
        Envia segmento FIN para iniciar encerramento de conexão.
        
        Args:
            dest_addr: Endereço de destino (host, porta)
        """
        segment = TCPSegment()
        segment.source_port = self.socket_ref.local_port
        segment.dest_port = dest_addr[1]
        segment.seq_num = self.seq_num
        segment.ack_num = self.ack_num
        segment.set_flag(TCP_FIN | TCP_ACK)
        segment.window_size = self.socket_ref._calculate_receive_window()
        
        self._send_segment(segment, dest_addr)
        
        # Atualiza números de sequência
        self.seq_num += 1  # FIN consome um número de sequência
    
    def send_fin_ack(self, dest_addr: Tuple[str, int]) -> None:
        """
        Envia segmento FIN-ACK em resposta a um FIN.
        
        Args:
            dest_addr: Endereço de destino (host, porta)
        """
        segment = TCPSegment()
        segment.source_port = self.socket_ref.local_port
        segment.dest_port = dest_addr[1]
        segment.seq_num = self.seq_num
        segment.ack_num = self.ack_num
        segment.set_flag(TCP_FIN | TCP_ACK)
        segment.window_size = self.socket_ref._calculate_receive_window()
        
        self._send_segment(segment, dest_addr)
        
        # Atualiza números de sequência
        self.seq_num += 1  # FIN consome um número de sequência
    
    def _send_segment(self, segment: TCPSegment, dest_addr: Tuple[str, int]) -> None:
        """
        Envia um segmento TCP através do socket UDP subjacente.
        
        Args:
            segment: Segmento TCP a enviar
            dest_addr: Endereço de destino
        """
        try:
            data = segment.pack()
            
            # Log detalhado do segmento enviado
            flags_str = []
            if segment.has_flag(TCP_SYN):
                flags_str.append("SYN")
            if segment.has_flag(TCP_ACK):
                flags_str.append("ACK")
            if segment.has_flag(TCP_FIN):
                flags_str.append("FIN")
            if segment.has_flag(TCP_RST):
                flags_str.append("RST")
            if segment.has_flag(TCP_PSH):
                flags_str.append("PSH")
            
            flags_display = ",".join(flags_str) if flags_str else "NONE"
            
            # Log detalhado do pacote enviado
            self.socket_ref._log_packet_transmission(segment, dest_addr, "SEND")
            
            self.socket_ref.udp_socket.sendto(data, dest_addr)
        except Exception as e:
            # Log do erro
            print(f"Erro ao enviar segmento: {e}")
    
    def handle_segment(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmento TCP recebido de acordo com o estado atual.
        
        Args:
            segment: Segmento TCP recebido
            addr: Endereço de origem do segmento
        """
        current_state = self.get_state()
        
        # Processa de acordo com o estado atual
        if current_state == self.CLOSED:
            self._handle_closed_state(segment, addr)
        elif current_state == self.LISTEN:
            self._handle_listen_state(segment, addr)
        elif current_state == self.SYN_SENT:
            self._handle_syn_sent_state(segment, addr)
        elif current_state == self.SYN_RCVD:
            self._handle_syn_rcvd_state(segment, addr)
        elif current_state == self.ESTABLISHED:
            self._handle_established_state(segment, addr)
        elif current_state == self.FIN_WAIT_1:
            self._handle_fin_wait_1_state(segment, addr)
        elif current_state == self.FIN_WAIT_2:
            self._handle_fin_wait_2_state(segment, addr)
        elif current_state == self.CLOSE_WAIT:
            self._handle_close_wait_state(segment, addr)
        elif current_state == self.CLOSING:
            self._handle_closing_state(segment, addr)
        elif current_state == self.LAST_ACK:
            self._handle_last_ack_state(segment, addr)
        elif current_state == self.TIME_WAIT:
            self._handle_time_wait_state(segment, addr)
    
    def _handle_closed_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """Processa segmentos no estado CLOSED."""
        # No estado CLOSED, responde com RST para qualquer segmento
        if not segment.has_flag(TCP_RST):
            self._send_rst_response(segment, addr)
    
    def _handle_listen_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado LISTEN.
        Implementa lado servidor do three-way handshake.
        """
        if segment.has_flag(TCP_SYN) and not segment.has_flag(TCP_ACK):
            # Recebeu SYN do cliente, inicia resposta do servidor
            
            # Armazena informações do cliente
            self.peer_seq_num = segment.seq_num
            self.ack_num = segment.seq_num + 1  # SYN consome um número de sequência
            
            # Gera ISN (Initial Sequence Number) aleatório conforme requisitos (0-1000)
            self.seq_num = random.randint(0, 1000)
            
            # Responde com SYN-ACK
            self.send_syn_ack(addr)
            
        elif segment.has_flag(TCP_RST):
            # Reset recebido, ignora
            pass
        # Ignora outros tipos de segmentos no estado LISTEN
    
    def _handle_syn_sent_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado SYN_SENT.
        Implementa lado cliente do three-way handshake.
        """
        if segment.has_flag(TCP_SYN) and segment.has_flag(TCP_ACK):
            # Recebeu SYN-ACK do servidor, verifica se ACK é válido
            if segment.ack_num == self.seq_num:
                # ACK válido, atualiza números de sequência
                self.peer_seq_num = segment.seq_num
                self.ack_num = segment.seq_num + 1  # SYN consome um número
                
                # Envia ACK final para completar three-way handshake
                self.send_ack(addr)
                
                # Transiciona para ESTABLISHED
                self.set_state(self.ESTABLISHED)
            else:
                # ACK inválido, ignora segmento
                pass
        elif segment.has_flag(TCP_SYN) and not segment.has_flag(TCP_ACK):
            # SYN simultâneo (caso especial - ambos lados iniciaram conexão)
            self.peer_seq_num = segment.seq_num
            self.ack_num = segment.seq_num + 1
            
            # Responde com SYN-ACK
            self.send_syn_ack(addr)
        elif segment.has_flag(TCP_RST):
            # Conexão recusada pelo servidor
            self.set_state(self.CLOSED)
        # Ignora outros tipos de segmentos no estado SYN_SENT
    
    def _handle_syn_rcvd_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado SYN_RCVD.
        Completa o three-way handshake do lado servidor.
        """
        if segment.has_flag(TCP_ACK) and not segment.has_flag(TCP_SYN):
            # Recebeu ACK final do three-way handshake
            if segment.ack_num == self.seq_num:
                # ACK válido, three-way handshake completo
                self.set_state(self.ESTABLISHED)
            else:
                # ACK inválido, ignora segmento
                pass
        elif segment.has_flag(TCP_SYN) and not segment.has_flag(TCP_ACK):
            # Retransmissão de SYN, reenvia SYN-ACK
            self.send_syn_ack(addr)
        elif segment.has_flag(TCP_RST):
            # Reset recebido, volta para LISTEN
            self.set_state(self.LISTEN)
        # Ignora outros tipos de segmentos no estado SYN_RCVD
    
    def _handle_established_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado ESTABLISHED.
        Implementa requisito 5.2: trata FIN recebido enviando ACK.
        """
        if segment.has_flag(TCP_FIN):
            # Requisito 5.2: Peer iniciou encerramento - trata FIN recebido enviando ACK
            print(f"FIN recebido do peer {addr} - iniciando encerramento passivo")
            
            # Processa dados se houver no mesmo segmento
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
            
            # Atualiza número de ACK para incluir o FIN (FIN consome 1 número de sequência)
            self.ack_num = segment.seq_num + len(segment.data) + 1
            
            # Envia ACK confirmando recebimento do FIN
            self.send_ack(addr)
            
            # Transiciona para CLOSE_WAIT - aguarda aplicação chamar close()
            self.set_state(self.CLOSE_WAIT)
            
            print(f"Transicionado para CLOSE_WAIT - aguardando close() da aplicação")
            
        elif segment.has_flag(TCP_ACK):
            # Processa ACK primeiro (pode ser ACK puro ou ACK com dados)
            self._process_ack(segment)
            
            # Se tem dados, processa também
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
    
    def _handle_fin_wait_1_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado FIN_WAIT_1.
        Implementa requisito 5.3: transiciona para CLOSED após ACK final.
        """
        if segment.has_flag(TCP_FIN) and segment.has_flag(TCP_ACK):
            # FIN-ACK simultâneo - peer enviou FIN e ACK do nosso FIN no mesmo segmento
            if segment.ack_num == self.seq_num:
                print(f"FIN-ACK simultâneo recebido de {addr}")
                
                # Processa dados se houver
                if len(segment.data) > 0:
                    self._process_data_segment(segment, addr)
                
                # Atualiza ACK para incluir o FIN do peer
                self.ack_num = segment.seq_num + len(segment.data) + 1
                
                # Envia ACK final
                self.send_ack(addr)
                
                # Vai direto para TIME_WAIT
                self._start_time_wait_timer()
                self.set_state(self.TIME_WAIT)
                
                print(f"Transicionado para TIME_WAIT após FIN-ACK simultâneo")
            else:
                print(f"FIN-ACK com número de ACK inválido: esperado {self.seq_num}, recebido {segment.ack_num}")
                
        elif segment.has_flag(TCP_ACK) and not segment.has_flag(TCP_FIN):
            # ACK do nosso FIN (sem FIN do peer ainda)
            if segment.ack_num == self.seq_num:
                print(f"ACK do nosso FIN recebido de {addr}")
                
                # Processa dados se houver
                if len(segment.data) > 0:
                    self._process_data_segment(segment, addr)
                
                # Transiciona para FIN_WAIT_2 - aguarda FIN do peer
                self.set_state(self.FIN_WAIT_2)
                
                print(f"Transicionado para FIN_WAIT_2 - aguardando FIN do peer")
            else:
                print(f"ACK com número inválido: esperado {self.seq_num}, recebido {segment.ack_num}")
                
        elif segment.has_flag(TCP_FIN) and not segment.has_flag(TCP_ACK):
            # FIN do peer (sem ACK do nosso FIN) - cruzamento de FINs
            print(f"FIN cruzado recebido de {addr}")
            
            # Processa dados se houver
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
            
            # Atualiza ACK para incluir o FIN do peer
            self.ack_num = segment.seq_num + len(segment.data) + 1
            
            # Envia ACK do FIN do peer
            self.send_ack(addr)
            
            # Transiciona para CLOSING - aguarda ACK do nosso FIN
            self.set_state(self.CLOSING)
            
            print(f"Transicionado para CLOSING após FIN cruzado")
        
        else:
            # Outros tipos de segmentos (dados, ACKs de dados, etc.)
            if segment.has_flag(TCP_ACK):
                self._process_ack(segment)
            
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
    
    def _handle_fin_wait_2_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado FIN_WAIT_2.
        Implementa requisito 5.3: transiciona para CLOSED após ACK final.
        """
        if segment.has_flag(TCP_FIN):
            # Recebeu FIN do peer - completa four-way handshake
            print(f"FIN do peer recebido em FIN_WAIT_2 de {addr}")
            
            # Processa dados se houver no mesmo segmento
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
            
            # Atualiza ACK para incluir o FIN (FIN consome 1 número de sequência)
            self.ack_num = segment.seq_num + len(segment.data) + 1
            
            # Envia ACK final do four-way handshake
            self.send_ack(addr)
            
            # Inicia TIME_WAIT antes de fechar definitivamente
            self._start_time_wait_timer()
            self.set_state(self.TIME_WAIT)
            
            print(f"Four-way handshake completo - transicionado para TIME_WAIT")
            
        else:
            # Outros tipos de segmentos (dados restantes, ACKs, etc.)
            if segment.has_flag(TCP_ACK):
                self._process_ack(segment)
            
            if len(segment.data) > 0:
                self._process_data_segment(segment, addr)
    
    def _handle_close_wait_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado CLOSE_WAIT.
        Implementa requisito 5.3: aguarda aplicação chamar close() para enviar próprio FIN.
        """
        # No estado CLOSE_WAIT, aguarda aplicação chamar close()
        # Pode ainda processar ACKs de dados pendentes e dados restantes
        
        if segment.has_flag(TCP_ACK):
            # Processa ACKs de dados que ainda estavam sendo enviados
            self._process_ack(segment)
        
        if len(segment.data) > 0:
            # Processa dados restantes que o peer ainda pode estar enviando
            self._process_data_segment(segment, addr)
        
        if segment.has_flag(TCP_FIN):
            # Retransmissão de FIN do peer - reenvia ACK
            print(f"Retransmissão de FIN recebida em CLOSE_WAIT de {addr}")
            self.send_ack(addr)
    
    def _handle_closing_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado CLOSING.
        Estado especial quando ambos os lados enviam FIN simultaneamente.
        Implementa requisito 5.3: transiciona para CLOSED após ACK final.
        """
        if segment.has_flag(TCP_ACK):
            # ACK do nosso FIN - completa encerramento simultâneo
            if segment.ack_num == self.seq_num:
                print(f"ACK final recebido em CLOSING de {addr} - encerramento simultâneo completo")
                
                # Inicia TIME_WAIT antes de fechar definitivamente
                self._start_time_wait_timer()
                self.set_state(self.TIME_WAIT)
                
                print(f"Transicionado para TIME_WAIT após encerramento simultâneo")
            else:
                print(f"ACK com número inválido em CLOSING: esperado {self.seq_num}, recebido {segment.ack_num}")
        
        elif segment.has_flag(TCP_FIN):
            # Retransmissão de FIN do peer - reenvia ACK
            print(f"Retransmissão de FIN recebida em CLOSING de {addr}")
            self.send_ack(addr)
        
        else:
            # Outros segmentos são ignorados no estado CLOSING
            print(f"Segmento ignorado em CLOSING: {segment}")
    
    def _handle_last_ack_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmentos no estado LAST_ACK.
        Implementa requisito 5.3: transiciona para CLOSED após ACK final.
        """
        if segment.has_flag(TCP_ACK):
            # ACK do nosso FIN - completa four-way handshake do lado passivo
            if segment.ack_num == self.seq_num:
                print(f"ACK final recebido em LAST_ACK de {addr} - four-way handshake completo")
                
                # Requisito 5.3: Transiciona para CLOSED após ACK final
                self.set_state(self.CLOSED)
                
                print(f"Conexão encerrada - transicionado para CLOSED")
            else:
                print(f"ACK com número inválido em LAST_ACK: esperado {self.seq_num}, recebido {segment.ack_num}")
        
        elif segment.has_flag(TCP_FIN):
            # Retransmissão de FIN do peer - reenvia ACK
            print(f"Retransmissão de FIN recebida em LAST_ACK de {addr}")
            self.send_ack(addr)
        
        else:
            # Outros segmentos são ignorados no estado LAST_ACK
            print(f"Segmento ignorado em LAST_ACK: {segment}")
    
    def _handle_time_wait_state(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """Processa segmentos no estado TIME_WAIT."""
        if segment.has_flag(TCP_FIN):
            # Retransmissão de FIN, reenvia ACK
            self.send_ack(addr)
            # Reinicia timer TIME_WAIT
            self._start_time_wait_timer()
    
    def _process_data_segment(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmento com dados.
        
        Args:
            segment: Segmento com dados
            addr: Endereço de origem
        """
        # Delega processamento para o método especializado do socket
        self.socket_ref._handle_received_data(segment, addr)
    
    def _process_ack(self, segment: TCPSegment) -> None:
        """
        Processa ACK recebido.
        Implementa acknowledgments cumulativos, atualiza RTT e remove dados confirmados.
        
        Args:
            segment: Segmento com ACK
        """
        # Delega processamento para o método especializado do socket
        self.socket_ref._handle_ack(segment)
    
    def _send_rst_response(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Envia resposta RST para segmento inválido.
        
        Args:
            segment: Segmento que causou o RST
            addr: Endereço de destino
        """
        rst_segment = TCPSegment()
        rst_segment.source_port = self.socket_ref.local_port
        rst_segment.dest_port = segment.source_port
        rst_segment.seq_num = 0
        rst_segment.ack_num = segment.seq_num + len(segment.data)
        rst_segment.set_flag(TCP_RST | TCP_ACK)
        rst_segment.window_size = 0
        
        self._send_segment(rst_segment, addr)
    
    def _start_time_wait_timer(self) -> None:
        """
        Inicia timer para estado TIME_WAIT (2 * MSL).
        """
        if self.time_wait_timer:
            self.time_wait_timer.cancel()
        
        # Timer de 60 segundos (2 * MSL simplificado)
        self.time_wait_timer = threading.Timer(60.0, self._time_wait_timeout)
        self.time_wait_timer.start()
    
    def _time_wait_timeout(self) -> None:
        """
        Callback para timeout do TIME_WAIT.
        """
        self.set_state(self.CLOSED)
    
    def initiate_close(self) -> None:
        """
        Inicia o encerramento da conexão enviando FIN.
        """
        current_state = self.get_state()
        
        if current_state == self.ESTABLISHED:
            self.send_fin(self.peer_address)
            self.set_state(self.FIN_WAIT_1)
        elif current_state == self.CLOSE_WAIT:
            self.send_fin(self.peer_address)
            self.set_state(self.LAST_ACK)
    
    def wait_for_connection(self, timeout: Optional[float] = None) -> bool:
        """
        Aguarda estabelecimento da conexão.
        
        Args:
            timeout: Timeout em segundos (None para aguardar indefinidamente)
            
        Returns:
            bool: True se conexão foi estabelecida
        """
        return self.connection_established.wait(timeout)
    
    def wait_for_close(self, timeout: Optional[float] = None) -> bool:
        """
        Aguarda encerramento da conexão.
        
        Args:
            timeout: Timeout em segundos (None para aguardar indefinidamente)
            
        Returns:
            bool: True se conexão foi encerrada
        """
        return self.connection_closed.wait(timeout)
    
    def cleanup(self) -> None:
        """
        Limpa recursos do gerenciador de conexão.
        """
        if self.time_wait_timer:
            self.time_wait_timer.cancel()
            self.time_wait_timer = None
        
        self.connection_established.clear()
        self.connection_closed.set()
        self.set_state(self.CLOSED)
    
    def __str__(self) -> str:
        """
        Representação string do gerenciador de conexão.
        """
        return (f"ConnectionManager(state={self.state}, seq={self.seq_num}, "
                f"ack={self.ack_num}, peer={self.peer_address})")


class SimpleTCPSocket:
    """
    Implementação de TCP simplificado sobre UDP.
    
    Fornece interface similar ao TCP padrão com estabelecimento de conexão,
    controle de fluxo, retransmissão e encerramento controlado.
    """
    
    def __init__(self, port: int = 0):
        """
        Inicializa o socket TCP simplificado.
        
        Args:
            port: Porta local para bind (0 para porta automática)
        """
        # Configura logger específico para este socket usando o sistema do projeto
        self.protocol_logger = ProtocolLogger(f'SimpleTCP-{port}')
        self.protocol_logger.start_session()
        # Socket UDP subjacente
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind na porta especificada
        self.local_port = port
        if port == 0:
            # Porta automática
            self.udp_socket.bind(('', 0))
            self.local_port = self.udp_socket.getsockname()[1]
        else:
            self.udp_socket.bind(('', port))
        
        # Buffers de envio e recepção (4KB cada conforme especificação)
        self.send_buffer = BufferManager(4096)
        self.receive_buffer = BufferManager(4096)
        
        # Componentes de controle
        self.rtt_manager = RTTManager()
        self.connection_manager = ConnectionManager(self)
        
        # Números de sequência iniciais aleatórios (0-1000 conforme requisitos)
        self.initial_seq_num = random.randint(0, 1000)
        self.connection_manager.seq_num = self.initial_seq_num
        
        # Thread de recepção
        self._receive_thread: Optional[threading.Thread] = None
        self._running = False
        self._receive_lock = threading.Lock()
        
        # Controle de janela de recepção (Requisito 3.1 - janela inicial de 4096 bytes)
        self.receive_window_size = 4096  # Janela inicial de 4KB
        self.advertised_window_size = 4096  # Janela anunciada para o peer
        
        # Estado de conexão
        self._connected = False
        self._listening = False
        
        # Fila de conexões pendentes (para servidor)
        self._pending_connections = []
        self._pending_lock = threading.Lock()
        
        # Timeout padrão para operações
        self.default_timeout = 30.0
        
        # Controle de envio e retransmissão
        self.max_segment_size = 1024  # MSS de 1KB
        self.unacked_segments = {}  # {seq_num: (segment, timestamp, timer, retransmit_count)}
        self.send_lock = threading.Lock()
        self.peer_window_size = 4096  # Janela do receptor (inicialmente 4KB)
        self.last_byte_sent = 0
        self.last_byte_acked = 0
        
        # Controle de retransmissão (Requisitos 4.2, 4.3)
        self.max_retransmit_attempts = 5  # Limite de tentativas de retransmissão
        self.retransmit_backoff_factor = 2.0  # Fator de backoff exponencial
        self.duplicate_ack_count = 0  # Contador de ACKs duplicados
        self.last_ack_received = 0  # Último ACK recebido
        self.fast_retransmit_threshold = 3  # Threshold para fast retransmit
        
        # Controle de recepção e reordenação
        self.out_of_order_segments = {}  # {seq_num: segment_data}
        self.receive_lock = threading.Lock()
        self.next_expected_seq = 0  # Próximo número de sequência esperado
    
    def _log_packet_transmission(self, segment: TCPSegment, dest_addr: Tuple[str, int], direction: str = "SEND"):
        """
        Registra transmissão ou recepção de pacote no logger do protocolo.
        
        Args:
            segment: Segmento TCP
            dest_addr: Endereço de destino/origem
            direction: "SEND" ou "RECV"
        """
        # Determina flags do segmento
        flags = []
        if segment.has_flag(TCP_SYN):
            flags.append("SYN")
        if segment.has_flag(TCP_ACK):
            flags.append("ACK")
        if segment.has_flag(TCP_FIN):
            flags.append("FIN")
        if segment.has_flag(TCP_RST):
            flags.append("RST")
        if segment.has_flag(TCP_PSH):
            flags.append("PSH")
        
        packet_type = ",".join(flags) if flags else "DATA"
        
        # Registra no logger do protocolo
        if direction == "SEND":
            self.protocol_logger.log_transmission(
                packet_type=packet_type,
                seq_num=segment.seq_num,
                data_size=len(segment.data),
                protocol_overhead=20  # Tamanho do cabeçalho TCP
            )
        else:  # RECV
            self.protocol_logger.log_reception(
                packet_type=packet_type,
                seq_num=segment.seq_num,
                data_size=len(segment.data),
                success=True
            )
        
        # Log detalhado no console
        flags_display = ",".join(flags) if flags else "NONE"
        if direction == "SEND":
            print(f"[SEND] {self.local_port} -> {dest_addr[1]} | "
                  f"Seq={segment.seq_num} Ack={segment.ack_num} "
                  f"Flags=[{flags_display}] Win={segment.window_size} "
                  f"Len={len(segment.data)} bytes")
        else:
            print(f"[RECV] {dest_addr[1]} -> {self.local_port} | "
                  f"Seq={segment.seq_num} Ack={segment.ack_num} "
                  f"Flags=[{flags_display}] Win={segment.window_size} "
                  f"Len={len(segment.data)} bytes")
    
    def _log_retransmission(self, segment: TCPSegment, reason: str):
        """
        Registra retransmissão no logger do protocolo.
        
        Args:
            segment: Segmento retransmitido
            reason: Motivo da retransmissão
        """
        self.protocol_logger.log_retransmission(
            reason=reason,
            packet_type="DATA",
            seq_num=segment.seq_num
        )
    
    def _log_timeout(self, seq_num: int):
        """
        Registra timeout no logger do protocolo.
        
        Args:
            seq_num: Número de sequência que sofreu timeout
        """
        self.protocol_logger.log_timeout(seq_num=seq_num)
    
    def get_protocol_statistics(self) -> dict:
        """
        Retorna estatísticas do protocolo coletadas pelo logger.
        
        Returns:
            dict: Estatísticas completas do protocolo
        """
        return self.protocol_logger.get_statistics()
    
    def generate_protocol_report(self, detailed: bool = False) -> str:
        """
        Gera relatório de desempenho do protocolo.
        
        Args:
            detailed: Se deve incluir detalhes dos eventos
            
        Returns:
            str: Relatório formatado
        """
        return self.protocol_logger.generate_report(detailed=detailed)
    
    def _start_receive_thread(self) -> None:
        """
        Inicia thread de recepção de segmentos UDP.
        """
        if self._receive_thread is None or not self._receive_thread.is_alive():
            self._running = True
            self._receive_thread = threading.Thread(
                target=self._receive_loop,
                daemon=True,
                name=f"SimpleTCP-Receive-{self.local_port}"
            )
            self._receive_thread.start()
    
    def _stop_receive_thread(self) -> None:
        """
        Para thread de recepção.
        """
        self._running = False
        if self._receive_thread and self._receive_thread.is_alive():
            # Envia um pacote dummy para acordar o thread
            try:
                dummy_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                dummy_socket.sendto(b'', ('127.0.0.1', self.local_port))
                dummy_socket.close()
            except:
                pass
            
            self._receive_thread.join(timeout=1.0)
    
    def _receive_loop(self) -> None:
        """
        Loop principal de recepção de segmentos UDP.
        
        Implementa thread separada para receber segmentos UDP conforme requisito 2.2 e 2.5.
        Processa segmentos recebidos de acordo com o estado da conexão e implementa
        sincronização thread-safe com o ConnectionManager.
        
        A thread roda continuamente enquanto _running for True, processando:
        - Segmentos de controle (SYN, ACK, FIN)
        - Segmentos de dados
        - Timeouts para verificação periódica do estado _running
        """
        # Configura timeout para permitir verificação periódica de _running
        self.udp_socket.settimeout(1.0)
        
        while self._running:
            try:
                # Recebe segmento UDP (máximo 64KB)
                data, addr = self.udp_socket.recvfrom(65536)
                
                # Verifica novamente se ainda deve estar rodando
                if not self._running:
                    break
                
                # Processa segmento recebido de forma thread-safe
                self._process_received_segment(data, addr)
                
            except socket.timeout:
                # Timeout normal - permite verificação de _running
                continue
            except OSError as e:
                # Erro de socket (ex: socket fechado)
                if self._running:
                    # Log apenas se ainda deveria estar rodando
                    print(f"Erro na recepção: {e}")
                break
            except Exception as e:
                # Outros erros inesperados
                if self._running:
                    print(f"Erro inesperado na thread de recepção: {e}")
                # Continua tentando em caso de erro não crítico
                continue
    
    def _process_received_segment(self, data: bytes, addr: Tuple[str, int]) -> None:
        """
        Processa segmento TCP recebido de forma thread-safe.
        
        Implementa processamento de segmentos de acordo com o estado da conexão
        conforme requisitos 2.2 e 2.5. Utiliza sincronização thread-safe e
        integra com o ConnectionManager para processamento adequado.
        
        Args:
            data: Dados do segmento TCP recebido
            addr: Endereço de origem (host, porta)
        """
        try:
            # Deserializa segmento TCP
            segment = TCPSegment()
            segment.unpack(data)
            
            # Filtra segmentos destinados a outras portas
            if segment.dest_port != self.local_port:
                return
            
            # Log detalhado do segmento recebido
            flags_str = []
            if segment.has_flag(TCP_SYN):
                flags_str.append("SYN")
            if segment.has_flag(TCP_ACK):
                flags_str.append("ACK")
            if segment.has_flag(TCP_FIN):
                flags_str.append("FIN")
            if segment.has_flag(TCP_RST):
                flags_str.append("RST")
            if segment.has_flag(TCP_PSH):
                flags_str.append("PSH")
            
            flags_display = ",".join(flags_str) if flags_str else "NONE"
            
            # Log detalhado do pacote recebido
            self._log_packet_transmission(segment, addr, "RECV")
            
            # Processa segmento de forma thread-safe
            # O lock garante que apenas uma thread processe segmentos por vez
            with self._receive_lock:
                # Delega processamento para o ConnectionManager
                # que implementa a máquina de estados TCP
                self.connection_manager.handle_segment(segment, addr)
                
        except struct.error as e:
            # Erro de deserialização - segmento malformado
            print(f"Segmento malformado recebido de {addr}: {e}")
        except Exception as e:
            # Outros erros de processamento
            print(f"Erro ao processar segmento de {addr}: {e}")
    
    def connect(self, dest_address: Tuple[str, int]) -> None:
        """
        Estabelece conexão TCP com destino especificado.
        Implementa three-way handshake do lado cliente:
        1. Envia SYN inicial
        2. Aguarda SYN-ACK do servidor
        3. Envia ACK final e transiciona para ESTABLISHED
        
        Args:
            dest_address: Tupla (host, porta) do destino
            
        Raises:
            ConnectionRefused: Se conexão for recusada
            ConnectionTimeout: Se timeout ocorrer
            RuntimeError: Se socket já estiver conectado
        """
        if self._connected:
            raise RuntimeError("Socket já está conectado")
        
        if self.connection_manager.get_state() != ConnectionManager.CLOSED:
            raise RuntimeError("Socket não está no estado CLOSED")
        
        # Inicia thread de recepção
        self._start_receive_thread()
        
        # Gera número de sequência inicial aleatório (0-1000 conforme requisitos)
        self.initial_seq_num = random.randint(0, 1000)
        self.connection_manager.seq_num = self.initial_seq_num
        
        try:
            # Passo 1: Envia SYN inicial para iniciar three-way handshake
            self.connection_manager.send_syn(dest_address)
            
            # Passo 2: Aguarda estabelecimento da conexão (SYN-ACK + ACK automático)
            # O ConnectionManager processa SYN-ACK e envia ACK automaticamente
            if not self.connection_manager.wait_for_connection(self.default_timeout):
                # Limpa estado em caso de timeout
                self.connection_manager.set_state(ConnectionManager.CLOSED)
                self._stop_receive_thread()
                raise ConnectionTimeout(f"Timeout no estabelecimento da conexão com {dest_address}")
            
            # Passo 3: Conexão estabelecida com sucesso
            self._connected = True
            
            # Inicializa controle de sequência para dados
            self.next_expected_seq = self.connection_manager.ack_num
            self.last_byte_acked = self.connection_manager.seq_num
            
        except Exception as e:
            # Limpa recursos em caso de erro
            self.connection_manager.set_state(ConnectionManager.CLOSED)
            self._stop_receive_thread()
            raise e
    
    def listen(self) -> None:
        """
        Coloca socket em modo de escuta para conexões entrantes.
        Implementa modo de escuta do servidor para aceitar conexões TCP.
        
        Raises:
            RuntimeError: Se socket já estiver conectado ou não estiver no estado CLOSED
        """
        if self._connected:
            raise RuntimeError("Socket já está conectado")
        
        if self.connection_manager.get_state() != ConnectionManager.CLOSED:
            raise RuntimeError("Socket não está no estado CLOSED")
        
        # Inicia thread de recepção para processar segmentos entrantes
        self._start_receive_thread()
        
        # Configura estado para LISTEN - aguarda conexões entrantes
        self.connection_manager.set_state(ConnectionManager.LISTEN)
        self._listening = True
        
        # Limpa fila de conexões pendentes
        with self._pending_lock:
            self._pending_connections.clear()
    
    def accept(self) -> 'SimpleTCPSocket':
        """
        Aceita uma conexão entrante.
        Implementa lado servidor do three-way handshake:
        1. Aguarda SYN do cliente (processado automaticamente em LISTEN)
        2. Responde com SYN-ACK (processado automaticamente)
        3. Aguarda ACK final e transiciona para ESTABLISHED
        
        Returns:
            SimpleTCPSocket: Novo socket para a conexão estabelecida
            
        Raises:
            RuntimeError: Se não estiver em modo de escuta
            ConnectionTimeout: Se timeout ocorrer aguardando conexão
        """
        if not self._listening:
            raise RuntimeError("Socket não está em modo de escuta")
        
        if self.connection_manager.get_state() != ConnectionManager.LISTEN:
            raise RuntimeError("Socket não está no estado LISTEN")
        
        try:
            # Aguarda estabelecimento da conexão (three-way handshake completo)
            # O ConnectionManager processa SYN automaticamente e responde com SYN-ACK
            # Depois aguarda ACK final para transicionar para ESTABLISHED
            if not self.connection_manager.wait_for_connection(self.default_timeout):
                raise ConnectionTimeout("Timeout aguardando conexão entrante")
            
            # Conexão estabelecida com sucesso
            self._connected = True
            self._listening = False
            
            # Inicializa controle de sequência para dados
            self.next_expected_seq = self.connection_manager.ack_num
            self.last_byte_acked = self.connection_manager.seq_num
            
            # Em uma implementação completa, criaria um novo socket para a conexão
            # Por simplicidade, retorna este próprio socket
            return self
            
        except Exception as e:
            # Em caso de erro, volta para estado LISTEN
            self.connection_manager.set_state(ConnectionManager.LISTEN)
            raise e
    
    def send(self, data: bytes) -> int:
        """
        Envia dados através da conexão TCP.
        Implementa segmentação de dados, números de sequência corretos e timers para retransmissão.
        Respeita controle de fluxo conforme requisitos 3.3 e 3.4.
        
        Args:
            data: Dados a enviar
            
        Returns:
            int: Número de bytes enviados
            
        Raises:
            RuntimeError: Se não estiver conectado
        """
        if not self._connected or not self.connection_manager.can_send_data():
            raise RuntimeError("Socket não está conectado ou não pode enviar dados")
        
        if not data:
            return 0
        
        with self.send_lock:
            # Armazena dados no buffer de envio
            bytes_buffered = self.send_buffer.write(data)
            
            # Tenta enviar dados do buffer respeitando controle de fluxo
            if self._can_send_data():
                self._send_buffered_data()
            
            return bytes_buffered
    
    def _send_buffered_data(self) -> None:
        """
        Envia dados do buffer de envio respeitando controle de fluxo.
        Implementa requisitos 3.3 e 3.4: respeita window size do receptor e 
        verifica LastByteSent - LastByteAcked ≤ rwnd.
        Segmenta dados em pacotes TCP com números de sequência corretos.
        """
        while True:
            # Requisito 3.3 e 3.4: Verifica controle de fluxo
            # LastByteSent - LastByteAcked ≤ rwnd (receive window do peer)
            bytes_in_flight = self.last_byte_sent - self.last_byte_acked
            
            # Se janela do peer for 0, pausa envio (requisito 3.4)
            if self.peer_window_size == 0:
                break  # Janela fechada, aguarda window update
            
            # Verifica se ainda há espaço na janela do receptor
            if bytes_in_flight >= self.peer_window_size:
                break  # Janela cheia, aguarda ACKs para liberar espaço
            
            # Calcula quantos bytes pode enviar respeitando a janela
            window_space = self.peer_window_size - bytes_in_flight
            max_segment_data = min(self.max_segment_size, window_space)
            
            # Verifica se há dados no buffer para enviar
            available_data = self.send_buffer.available_data()
            if available_data == 0:
                break  # Nenhum dado para enviar
            
            # Determina tamanho do segmento respeitando limitações
            segment_size = min(max_segment_data, available_data)
            if segment_size <= 0:
                break  # Não pode enviar nada no momento
            
            # Lê dados do buffer
            segment_data = self.send_buffer.read(segment_size)
            
            # Cria segmento TCP
            segment = TCPSegment()
            segment.source_port = self.local_port
            segment.dest_port = self.connection_manager.peer_address[1]
            segment.seq_num = self.connection_manager.seq_num
            segment.ack_num = self.connection_manager.ack_num
            segment.set_flag(TCP_ACK | TCP_PSH)  # ACK + PSH para dados
            segment.window_size = self._calculate_receive_window()
            segment.data = segment_data
            
            # Envia segmento
            try:
                packed_segment = segment.pack()
                self.udp_socket.sendto(packed_segment, self.connection_manager.peer_address)
                
                # Atualiza controle de envio
                self.last_byte_sent += len(segment_data)
                self.connection_manager.seq_num += len(segment_data)
                
                # Inicia timer para retransmissão
                self._start_retransmission_timer(segment)
                
            except Exception as e:
                # Recoloca dados no buffer em caso de erro
                self.send_buffer.write(segment_data)
                print(f"Erro ao enviar segmento: {e}")
                break
    
    def _start_retransmission_timer(self, segment: TCPSegment, retransmit_count: int = 0) -> None:
        """
        Inicia timer para retransmissão de segmento.
        Implementa detecção de timeout usando TimeoutInterval calculado (Requisitos 4.2, 4.3).
        
        Args:
            segment: Segmento TCP enviado
            retransmit_count: Número de retransmissões já realizadas
        """
        seq_num = segment.seq_num
        timestamp = time.time()
        
        # Calcula timeout interval com backoff exponencial para retransmissões
        base_timeout = self.rtt_manager.get_timeout_interval()
        if retransmit_count > 0:
            # Aplica backoff exponencial para retransmissões
            timeout_interval = base_timeout * (self.retransmit_backoff_factor ** retransmit_count)
            # Limita timeout máximo a 60 segundos
            timeout_interval = min(timeout_interval, 60.0)
        else:
            timeout_interval = base_timeout
        
        # Cancela timer anterior se existir
        if seq_num in self.unacked_segments:
            old_info = self.unacked_segments[seq_num]
            if len(old_info) >= 3 and old_info[2]:
                old_info[2].cancel()
        
        # Cria novo timer para detectar timeout
        timer = threading.Timer(timeout_interval, self._handle_timeout, args=[seq_num])
        timer.start()
        
        # Armazena informações do segmento incluindo contador de retransmissões
        self.unacked_segments[seq_num] = (segment, timestamp, timer, retransmit_count)
    
    def _handle_timeout(self, seq_num: int) -> None:
        """
        Trata timeout de segmento não confirmado.
        Implementa retransmissão automática com limite de tentativas (Requisito 4.3).
        
        Args:
            seq_num: Número de sequência do segmento que sofreu timeout
        """
        with self.send_lock:
            if seq_num not in self.unacked_segments:
                return  # Segmento já foi confirmado
            
            segment_info = self.unacked_segments[seq_num]
            if len(segment_info) < 4:
                # Formato antigo, assume 0 retransmissões
                segment, original_timestamp, timer = segment_info
                retransmit_count = 0
            else:
                segment, original_timestamp, timer, retransmit_count = segment_info
            
            # Verifica limite de tentativas de retransmissão
            if retransmit_count >= self.max_retransmit_attempts:
                # Excedeu limite de retransmissões - considera conexão perdida
                print(f"Segmento {seq_num} excedeu limite de retransmissões ({self.max_retransmit_attempts})")
                
                # Remove segmento da lista de não confirmados
                del self.unacked_segments[seq_num]
                
                # Em uma implementação completa, poderia fechar a conexão
                # Por ora, apenas remove o segmento
                return
            
            # Retransmite segmento
            try:
                packed_segment = segment.pack()
                self.udp_socket.sendto(packed_segment, self.connection_manager.peer_address)
                
                # Incrementa contador de retransmissões
                new_retransmit_count = retransmit_count + 1
                
                # Reinicia timer com backoff exponencial
                self._start_retransmission_timer(segment, new_retransmit_count)
                
                # Log da retransmissão
                self._log_retransmission(segment, f"timeout (tentativa {new_retransmit_count})")
                print(f"Retransmitindo segmento {seq_num} (tentativa {new_retransmit_count}/{self.max_retransmit_attempts})")
                
            except Exception as e:
                print(f"Erro na retransmissão do segmento {seq_num}: {e}")
                # Remove segmento da lista de não confirmados em caso de erro grave
                if seq_num in self.unacked_segments:
                    del self.unacked_segments[seq_num]
    
    def recv(self, buffer_size: int) -> bytes:
        """
        Recebe dados da conexão TCP.
        Implementa ordenação por número de sequência e trata segmentos fora de ordem.
        
        Args:
            buffer_size: Tamanho máximo do buffer de recepção
            
        Returns:
            bytes: Dados recebidos ordenados por sequência
            
        Raises:
            RuntimeError: Se não estiver conectado
        """
        if not self._connected or not self.connection_manager.can_receive_data():
            raise RuntimeError("Socket não está conectado ou não pode receber dados")
        
        if buffer_size <= 0:
            return b''
        
        with self.receive_lock:
            # Processa segmentos fora de ordem que agora podem estar em sequência
            self._process_out_of_order_segments()
            
            # Lê dados do buffer de recepção
            return self.receive_buffer.read(buffer_size)
    
    def _process_out_of_order_segments(self) -> None:
        """
        Processa segmentos fora de ordem que agora podem estar em sequência.
        Move dados ordenados para o buffer de recepção.
        """
        while self.next_expected_seq in self.out_of_order_segments:
            # Encontrou o próximo segmento esperado
            segment_data = self.out_of_order_segments.pop(self.next_expected_seq)
            
            # Move dados para buffer de recepção
            bytes_written = self.receive_buffer.write(segment_data)
            
            if bytes_written > 0:
                # Atualiza próximo número de sequência esperado
                self.next_expected_seq += len(segment_data)
            else:
                # Buffer cheio, recoloca segmento
                self.out_of_order_segments[self.next_expected_seq] = segment_data
                break
    
    def _handle_received_data(self, segment: TCPSegment, addr: Tuple[str, int]) -> None:
        """
        Processa segmento com dados recebido.
        Implementa ordenação por número de sequência e buffering de segmentos fora de ordem.
        
        Args:
            segment: Segmento TCP com dados
            addr: Endereço de origem
        """
        if not segment.data:
            return  # Segmento sem dados
        
        with self.receive_lock:
            seq_num = segment.seq_num
            
            if seq_num == self.next_expected_seq:
                # Segmento em ordem - processa imediatamente
                bytes_written = self.receive_buffer.write(segment.data)
                
                if bytes_written > 0:
                    # Atualiza números de sequência
                    self.next_expected_seq += len(segment.data)
                    self.connection_manager.ack_num = self.next_expected_seq
                    
                    # Processa segmentos fora de ordem que agora podem estar em sequência
                    self._process_out_of_order_segments()
                    
                    # Envia ACK confirmando recebimento
                    self.connection_manager.send_ack(addr)
                else:
                    # Buffer cheio - envia ACK com janela 0
                    self.connection_manager.send_ack(addr)
                    
            elif seq_num > self.next_expected_seq:
                # Segmento fora de ordem (futuro) - armazena para reordenação
                if seq_num not in self.out_of_order_segments:
                    self.out_of_order_segments[seq_num] = segment.data
                
                # Envia ACK duplicado com último número em ordem
                self.connection_manager.send_ack(addr)
                
            else:
                # Segmento duplicado ou muito antigo - envia ACK e ignora
                self.connection_manager.send_ack(addr)
    
    def _handle_ack(self, segment: TCPSegment) -> None:
        """
        Processa ACK recebido.
        Implementa acknowledgments cumulativos, atualiza estimativas de RTT e remove dados confirmados.
        Atualiza janela do peer e tenta enviar mais dados se a janela abrir.
        Implementa detecção de ACKs duplicados para fast retransmit.
        
        Args:
            segment: Segmento com ACK
        """
        ack_num = segment.ack_num
        old_window_size = self.peer_window_size
        
        with self.send_lock:
            # Atualiza janela do peer (requisito 3.3)
            self.peer_window_size = segment.window_size
            
            # Detecta ACKs duplicados para fast retransmit
            if ack_num == self.last_ack_received:
                self.duplicate_ack_count += 1
                
                # Fast retransmit: se receber 3 ACKs duplicados, retransmite imediatamente
                if self.duplicate_ack_count >= self.fast_retransmit_threshold:
                    self._handle_fast_retransmit(ack_num)
                    self.duplicate_ack_count = 0  # Reset contador
            else:
                # ACK novo, reset contador de duplicados
                self.duplicate_ack_count = 0
                self.last_ack_received = ack_num
            
            # Processa acknowledgments cumulativos
            segments_to_remove = []
            
            for seq_num in list(self.unacked_segments.keys()):
                if seq_num < ack_num:
                    # Segmento foi confirmado
                    segment_info = self.unacked_segments[seq_num]
                    
                    # Extrai informações do segmento (compatível com formato antigo e novo)
                    if len(segment_info) >= 4:
                        segment_obj, timestamp, timer, retransmit_count = segment_info
                    else:
                        segment_obj, timestamp, timer = segment_info
                        retransmit_count = 0
                    
                    # Cancela timer de retransmissão
                    if timer:
                        timer.cancel()
                    
                    # Calcula RTT sample apenas para segmentos não retransmitidos
                    # (evita distorção das estimativas de RTT)
                    if retransmit_count == 0:
                        sample_rtt = time.time() - timestamp
                        self.rtt_manager.update_rtt(sample_rtt)
                    
                    # Marca para remoção
                    segments_to_remove.append(seq_num)
            
            # Remove segmentos confirmados
            for seq_num in segments_to_remove:
                del self.unacked_segments[seq_num]
            
            # Atualiza controle de fluxo
            window_opened = False
            if ack_num > self.last_byte_acked:
                self.last_byte_acked = ack_num
                window_opened = True
            
            # Verifica se janela do peer abriu (window update)
            if self.peer_window_size > old_window_size or window_opened:
                # Tenta enviar mais dados se houver espaço na janela
                if self._can_send_data():
                    self._send_buffered_data()
    
    def _handle_fast_retransmit(self, ack_num: int) -> None:
        """
        Implementa fast retransmit quando recebe ACKs duplicados.
        Retransmite o próximo segmento não confirmado imediatamente.
        
        Args:
            ack_num: Número de ACK duplicado recebido
        """
        # Encontra o próximo segmento não confirmado para retransmitir
        next_seq_to_retransmit = None
        
        for seq_num in sorted(self.unacked_segments.keys()):
            if seq_num >= ack_num:
                next_seq_to_retransmit = seq_num
                break
        
        if next_seq_to_retransmit is not None:
            segment_info = self.unacked_segments[next_seq_to_retransmit]
            
            if len(segment_info) >= 4:
                segment, timestamp, timer, retransmit_count = segment_info
            else:
                segment, timestamp, timer = segment_info
                retransmit_count = 0
            
            # Verifica se ainda não excedeu limite de retransmissões
            if retransmit_count < self.max_retransmit_attempts:
                try:
                    # Retransmite segmento imediatamente (fast retransmit)
                    packed_segment = segment.pack()
                    self.udp_socket.sendto(packed_segment, self.connection_manager.peer_address)
                    
                    # Cancela timer anterior
                    if timer:
                        timer.cancel()
                    
                    # Incrementa contador e reinicia timer
                    new_retransmit_count = retransmit_count + 1
                    self._start_retransmission_timer(segment, new_retransmit_count)
                    
                    print(f"Fast retransmit do segmento {next_seq_to_retransmit} (tentativa {new_retransmit_count})")
                    
                except Exception as e:
                    print(f"Erro no fast retransmit do segmento {next_seq_to_retransmit}: {e}")
    
    def close(self) -> None:
        """
        Encerra a conexão TCP de forma controlada.
        Implementa requisitos 5.1, 5.4, 5.5:
        - Garante transmissão de dados pendentes antes de enviar FIN
        - Inicia four-way handshake enviando FIN
        - Gerencia estados de encerramento adequadamente
        
        Raises:
            RuntimeError: Se ocorrer erro durante o encerramento
        """
        if not self._connected:
            # Socket já está fechado, apenas limpa recursos
            self._cleanup_resources()
            return
        
        current_state = self.connection_manager.get_state()
        
        # Verifica se está em estado válido para encerramento
        if current_state not in {ConnectionManager.ESTABLISHED, ConnectionManager.CLOSE_WAIT}:
            print(f"Aviso: Tentativa de fechar conexão em estado {current_state}")
            self._cleanup_resources()
            return
        
        try:
            # Requisito 5.1: Garante transmissão de dados pendentes
            self._ensure_pending_data_transmitted()
            
            # Requisito 5.4: Inicia four-way handshake enviando FIN
            # Requisito 5.5: Gerencia estados de encerramento
            if current_state == ConnectionManager.ESTABLISHED:
                # Lado ativo do encerramento - envia FIN primeiro
                self._initiate_active_close()
            elif current_state == ConnectionManager.CLOSE_WAIT:
                # Lado passivo do encerramento - responde ao FIN recebido
                self._initiate_passive_close()
            
            # Aguarda encerramento completo com timeout
            close_timeout = min(self.default_timeout, 30.0)  # Máximo 30s para encerramento
            if not self.connection_manager.wait_for_close(close_timeout):
                print(f"Timeout no encerramento da conexão após {close_timeout}s")
                # Força encerramento mesmo com timeout
                self.connection_manager.set_state(ConnectionManager.CLOSED)
            
            self._connected = False
            
        except Exception as e:
            print(f"Erro durante encerramento da conexão: {e}")
            # Força encerramento em caso de erro
            self.connection_manager.set_state(ConnectionManager.CLOSED)
            self._connected = False
        finally:
            # Sempre limpa recursos independente do resultado
            self._cleanup_resources()
    
    def _ensure_pending_data_transmitted(self) -> None:
        """
        Garante que todos os dados pendentes sejam transmitidos antes do encerramento.
        Implementa requisito 5.1: transmissão de dados pendentes.
        """
        max_wait_time = 10.0  # Máximo 10 segundos para esvaziar buffers
        start_time = time.time()
        
        # Aguarda buffer de envio esvaziar (dados serem enviados)
        while (self.send_buffer.available_data() > 0 and 
               time.time() - start_time < max_wait_time):
            # Força envio de dados pendentes se possível
            if self._can_send_data():
                with self.send_lock:
                    self._send_buffered_data()
            time.sleep(0.1)
        
        # Aguarda confirmação de todos os dados enviados (ACKs)
        remaining_wait = max_wait_time - (time.time() - start_time)
        if remaining_wait > 0:
            ack_start_time = time.time()
            while (len(self.unacked_segments) > 0 and 
                   time.time() - ack_start_time < remaining_wait):
                time.sleep(0.1)
        
        # Log de dados não transmitidos (para debug)
        if self.send_buffer.available_data() > 0:
            print(f"Aviso: {self.send_buffer.available_data()} bytes não enviados no buffer")
        
        if len(self.unacked_segments) > 0:
            print(f"Aviso: {len(self.unacked_segments)} segmentos não confirmados")
    
    def _initiate_active_close(self) -> None:
        """
        Inicia encerramento ativo da conexão (lado que chama close() primeiro).
        Implementa requisitos 5.4 e 5.5: envia FIN e gerencia transição de estado.
        """
        if self.connection_manager.peer_address:
            # Envia FIN para iniciar four-way handshake
            self.connection_manager.send_fin(self.connection_manager.peer_address)
            
            # Transiciona para FIN_WAIT_1
            self.connection_manager.set_state(ConnectionManager.FIN_WAIT_1)
            
            print(f"Iniciado encerramento ativo - enviado FIN, estado: {ConnectionManager.FIN_WAIT_1}")
        else:
            print("Erro: Endereço do peer não disponível para envio de FIN")
            raise RuntimeError("Não é possível enviar FIN - endereço do peer desconhecido")
    
    def _initiate_passive_close(self) -> None:
        """
        Inicia encerramento passivo da conexão (resposta ao FIN recebido).
        Implementa requisitos 5.2 e 5.3: envia FIN após receber FIN do peer.
        """
        if self.connection_manager.peer_address:
            # Envia nosso FIN em resposta ao FIN recebido
            self.connection_manager.send_fin(self.connection_manager.peer_address)
            
            # Transiciona para LAST_ACK
            self.connection_manager.set_state(ConnectionManager.LAST_ACK)
            
            print(f"Iniciado encerramento passivo - enviado FIN, estado: {ConnectionManager.LAST_ACK}")
        else:
            print("Erro: Endereço do peer não disponível para envio de FIN")
            raise RuntimeError("Não é possível enviar FIN - endereço do peer desconhecido")
    
    def _cleanup_resources(self) -> None:
        """
        Limpa todos os recursos do socket.
        """
        # Limpa segmentos não confirmados e timers de retransmissão
        self.clear_unacked_segments()
        
        # Para thread de recepção
        self._stop_receive_thread()
        
        # Fecha socket UDP
        try:
            self.udp_socket.close()
        except:
            pass
        
        # Limpa recursos do gerenciador de conexão
        self.connection_manager.cleanup()
        
        # Limpa buffers
        self.send_buffer.clear()
        self.receive_buffer.clear()
        self.out_of_order_segments.clear()
    
    def get_local_address(self) -> Tuple[str, int]:
        """
        Retorna endereço local do socket.
        
        Returns:
            Tuple[str, int]: Tupla (host, porta) local
        """
        return self.udp_socket.getsockname()
    
    def get_peer_address(self) -> Optional[Tuple[str, int]]:
        """
        Retorna endereço do peer conectado.
        
        Returns:
            Optional[Tuple[str, int]]: Tupla (host, porta) do peer ou None
        """
        return self.connection_manager.peer_address
    
    def is_connected(self) -> bool:
        """
        Verifica se socket está conectado.
        
        Returns:
            bool: True se conectado
        """
        return self._connected and self.connection_manager.is_connected()
    
    def get_connection_state(self) -> str:
        """
        Retorna estado atual da conexão.
        
        Returns:
            str: Estado da conexão TCP
        """
        return self.connection_manager.get_state()
    
    def get_rtt_stats(self) -> dict:
        """
        Retorna estatísticas de RTT.
        
        Returns:
            dict: Dicionário com estatísticas de RTT
        """
        return {
            'estimated_rtt': self.rtt_manager.get_estimated_rtt(),
            'dev_rtt': self.rtt_manager.get_dev_rtt(),
            'timeout_interval': self.rtt_manager.get_timeout_interval()
        }
    
    def _calculate_receive_window(self) -> int:
        """
        Calcula o tamanho da janela de recepção baseado no espaço disponível no buffer.
        Implementa requisitos 3.1, 3.2 e 3.5 para anunciar window size correto.
        
        Returns:
            int: Tamanho da janela de recepção em bytes
        """
        # Calcula espaço disponível no buffer de recepção
        available_space = self.receive_buffer.available_space()
        
        # A janela anunciada é o espaço disponível no buffer
        # Requisito 3.5: quando buffer está cheio, anuncia janela 0
        self.advertised_window_size = available_space
        
        return available_space
    
    def _can_send_data(self) -> bool:
        """
        Verifica se é possível enviar dados respeitando controle de fluxo.
        Implementa requisito 3.3: LastByteSent - LastByteAcked ≤ rwnd
        
        Returns:
            bool: True se pode enviar dados
        """
        # Verifica se há espaço na janela do receptor
        bytes_in_flight = self.last_byte_sent - self.last_byte_acked
        return bytes_in_flight < self.peer_window_size and self.peer_window_size > 0
    
    def _get_available_send_window(self) -> int:
        """
        Calcula quantos bytes podem ser enviados respeitando a janela do receptor.
        
        Returns:
            int: Número de bytes que podem ser enviados
        """
        bytes_in_flight = self.last_byte_sent - self.last_byte_acked
        return max(0, self.peer_window_size - bytes_in_flight)
    
    def get_buffer_stats(self) -> dict:
        """
        Retorna estatísticas dos buffers.
        
        Returns:
            dict: Dicionário com estatísticas dos buffers
        """
        return {
            'send_buffer': {
                'available_space': self.send_buffer.available_space(),
                'available_data': self.send_buffer.available_data(),
                'size': self.send_buffer.size
            },
            'receive_buffer': {
                'available_space': self.receive_buffer.available_space(),
                'available_data': self.receive_buffer.available_data(),
                'size': self.receive_buffer.size
            },
            'receive_window': self.advertised_window_size,
            'peer_window': self.peer_window_size
        }
    
    def get_flow_control_stats(self) -> dict:
        """
        Retorna estatísticas de controle de fluxo.
        
        Returns:
            dict: Dicionário com estatísticas de controle de fluxo
        """
        bytes_in_flight = self.last_byte_sent - self.last_byte_acked
        return {
            'last_byte_sent': self.last_byte_sent,
            'last_byte_acked': self.last_byte_acked,
            'bytes_in_flight': bytes_in_flight,
            'peer_window_size': self.peer_window_size,
            'advertised_window_size': self.advertised_window_size,
            'available_send_window': self._get_available_send_window(),
            'can_send_data': self._can_send_data(),
            'unacked_segments_count': len(self.unacked_segments)
        }
    
    def get_retransmission_stats(self) -> dict:
        """
        Retorna estatísticas do sistema de retransmissão.
        
        Returns:
            dict: Dicionário com estatísticas de retransmissão
        """
        with self.send_lock:
            total_retransmissions = 0
            segments_by_retransmit_count = {}
            
            for seq_num, segment_info in self.unacked_segments.items():
                if len(segment_info) >= 4:
                    retransmit_count = segment_info[3]
                else:
                    retransmit_count = 0
                
                total_retransmissions += retransmit_count
                
                if retransmit_count not in segments_by_retransmit_count:
                    segments_by_retransmit_count[retransmit_count] = 0
                segments_by_retransmit_count[retransmit_count] += 1
            
            return {
                'unacked_segments_count': len(self.unacked_segments),
                'total_retransmissions': total_retransmissions,
                'segments_by_retransmit_count': segments_by_retransmit_count,
                'max_retransmit_attempts': self.max_retransmit_attempts,
                'retransmit_backoff_factor': self.retransmit_backoff_factor,
                'current_timeout_interval': self.rtt_manager.get_timeout_interval()
            }
    
    def set_retransmission_params(self, max_attempts: int = None, backoff_factor: float = None) -> None:
        """
        Configura parâmetros do sistema de retransmissão.
        
        Args:
            max_attempts: Número máximo de tentativas de retransmissão
            backoff_factor: Fator de backoff exponencial para timeouts
        """
        if max_attempts is not None and max_attempts > 0:
            self.max_retransmit_attempts = max_attempts
        
        if backoff_factor is not None and backoff_factor > 1.0:
            self.retransmit_backoff_factor = backoff_factor
    
    def retransmit_all_unacked(self) -> int:
        """
        Força retransmissão de todos os segmentos não confirmados.
        Útil para recuperação de falhas de rede ou testes.
        
        Returns:
            int: Número de segmentos retransmitidos
        """
        retransmitted_count = 0
        
        with self.send_lock:
            for seq_num in list(self.unacked_segments.keys()):
                segment_info = self.unacked_segments[seq_num]
                
                if len(segment_info) >= 4:
                    segment, timestamp, timer, retransmit_count = segment_info
                else:
                    segment, timestamp, timer = segment_info
                    retransmit_count = 0
                
                # Verifica se ainda não excedeu limite de retransmissões
                if retransmit_count < self.max_retransmit_attempts:
                    try:
                        # Retransmite segmento
                        packed_segment = segment.pack()
                        self.udp_socket.sendto(packed_segment, self.connection_manager.peer_address)
                        
                        # Cancela timer anterior
                        if timer:
                            timer.cancel()
                        
                        # Incrementa contador e reinicia timer
                        new_retransmit_count = retransmit_count + 1
                        self._start_retransmission_timer(segment, new_retransmit_count)
                        
                        retransmitted_count += 1
                        
                    except Exception as e:
                        print(f"Erro na retransmissão forçada do segmento {seq_num}: {e}")
                else:
                    # Remove segmentos que excederam limite
                    print(f"Removendo segmento {seq_num} que excedeu limite de retransmissões")
                    del self.unacked_segments[seq_num]
        
        if retransmitted_count > 0:
            print(f"Retransmitidos {retransmitted_count} segmentos não confirmados")
        
        return retransmitted_count
    
    def clear_unacked_segments(self) -> None:
        """
        Limpa todos os segmentos não confirmados e seus timers.
        """
        with self.send_lock:
            for seq_num, segment_info in list(self.unacked_segments.items()):
                if len(segment_info) >= 3 and segment_info[2]:
                    # Cancela timer se existir
                    segment_info[2].cancel()
            
            self.unacked_segments.clear()
    
    def finalize_logging(self):
        """
        Finaliza a sessão de logging e retorna estatísticas.
        
        Returns:
            dict: Estatísticas completas do protocolo
        """
        self.protocol_logger.end_session()
        return self.protocol_logger.get_statistics()
    
    def export_protocol_logs(self, filename: str):
        """
        Exporta logs do protocolo para arquivo CSV.
        
        Args:
            filename: Nome do arquivo CSV
        """
        self.protocol_logger.export_events_csv(filename)
        with self.send_lock:
            for seq_num, segment_info in self.unacked_segments.items():
                if len(segment_info) >= 3 and segment_info[2]:
                    segment_info[2].cancel()  # Cancela timer
            
            self.unacked_segments.clear()
            self.duplicate_ack_count = 0
            self.last_ack_received = 0
    
    def set_timeout(self, timeout: float) -> None:
        """
        Define timeout padrão para operações.
        
        Args:
            timeout: Timeout em segundos
        """
        self.default_timeout = max(0.1, timeout)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __str__(self) -> str:
        """
        Representação string do socket.
        """
        local_addr = self.get_local_address()
        peer_addr = self.get_peer_address()
        state = self.get_connection_state()
        
        return (f"SimpleTCPSocket(local={local_addr}, peer={peer_addr}, "
                f"state={state}, connected={self.is_connected()})")
    
    def __repr__(self) -> str:
        """
        Representação detalhada do socket.
        """
        return self.__str__()