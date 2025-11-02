"""
Módulo para manipulação de pacotes RDT com serialização/desserialização.
Implementa classes base e específicas para diferentes versões do protocolo RDT.
"""

import struct
import hashlib
from typing import Optional, Union


class PacketError(Exception):
    """Exceção base para erros relacionados a pacotes."""
    pass


class Packet:
    """
    Classe base para pacotes RDT com funcionalidades de serialização/desserialização.
    
    Tipos de pacote suportados:
    - DATA (0): Pacote de dados
    - ACK (1): Confirmação positiva
    - NAK (2): Confirmação negativa
    """
    
    # Constantes para tipos de pacote
    DATA = 0
    ACK = 1
    NAK = 2
    
    VALID_TYPES = {DATA, ACK, NAK}
    
    def __init__(self, packet_type: int, data: bytes = b''):
        """
        Inicializa um pacote base.
        
        Args:
            packet_type: Tipo do pacote (DATA=0, ACK=1, NAK=2)
            data: Dados do pacote (bytes)
            
        Raises:
            PacketError: Se o tipo de pacote for inválido
        """
        if packet_type not in self.VALID_TYPES:
            raise PacketError(f"Tipo de pacote inválido: {packet_type}. Deve ser DATA(0), ACK(1) ou NAK(2)")
        
        self.type = packet_type
        self.data = data if isinstance(data, bytes) else data.encode('utf-8')
    
    def serialize(self) -> bytes:
        """
        Serializa o pacote para formato bytes para transmissão.
        
        Formato base: | Tipo (1 byte) | Dados (variável) |
        
        Returns:
            bytes: Pacote serializado
        """
        try:
            # Formato: tipo (1 byte) + dados
            return struct.pack('!B', self.type) + self.data
        except struct.error as e:
            raise PacketError(f"Erro na serialização do pacote: {e}")
    
    @classmethod
    def deserialize(cls, raw_data: bytes) -> 'Packet':
        """
        Reconstrói um pacote a partir de bytes recebidos.
        
        Args:
            raw_data: Dados brutos recebidos
            
        Returns:
            Packet: Instância do pacote reconstruído
            
        Raises:
            PacketError: Se os dados estiverem malformados
        """
        if not isinstance(raw_data, bytes):
            raise PacketError("Dados devem ser do tipo bytes")
        
        if len(raw_data) < 1:
            raise PacketError("Dados insuficientes para formar um pacote válido")
        
        try:
            # Extrai tipo (1 byte)
            packet_type = struct.unpack('!B', raw_data[:1])[0]
            
            # Valida tipo
            if packet_type not in cls.VALID_TYPES:
                raise PacketError(f"Tipo de pacote inválido nos dados: {packet_type}")
            
            # Extrai dados restantes
            data = raw_data[1:]
            
            return cls(packet_type, data)
            
        except struct.error as e:
            raise PacketError(f"Erro na desserialização: dados malformados - {e}")
    
    def is_data_packet(self) -> bool:
        """Verifica se é um pacote de dados."""
        return self.type == self.DATA
    
    def is_ack_packet(self) -> bool:
        """Verifica se é um pacote ACK."""
        return self.type == self.ACK
    
    def is_nak_packet(self) -> bool:
        """Verifica se é um pacote NAK."""
        return self.type == self.NAK
    
    def get_type_name(self) -> str:
        """Retorna o nome do tipo do pacote."""
        type_names = {self.DATA: "DATA", self.ACK: "ACK", self.NAK: "NAK"}
        return type_names.get(self.type, "UNKNOWN")
    
    def __str__(self) -> str:
        """Representação string do pacote."""
        return f"Packet(type={self.get_type_name()}, data_len={len(self.data)})"
    
    def __repr__(self) -> str:
        """Representação detalhada do pacote."""
        return f"Packet(type={self.type}, data={self.data[:20]}{'...' if len(self.data) > 20 else ''})"


class RDT20Packet(Packet):
    """
    Pacote RDT 2.0 com checksum para detecção de erros.
    
    Formato: | Tipo (1 byte) | Checksum (4 bytes) | Dados (variável) |
    """
    
    def __init__(self, packet_type: int, data: bytes = b'', checksum: Optional[bytes] = None):
        """
        Inicializa um pacote RDT 2.0.
        
        Args:
            packet_type: Tipo do pacote
            data: Dados do pacote
            checksum: Checksum pré-calculado (opcional)
        """
        super().__init__(packet_type, data)
        self.checksum = checksum or self._calculate_checksum()
    
    def _calculate_checksum(self) -> bytes:
        """
        Calcula checksum MD5 dos dados do pacote.
        
        Returns:
            bytes: Primeiros 4 bytes do hash MD5
        """
        # Combina tipo e dados para o checksum
        content = struct.pack('!B', self.type) + self.data
        md5_hash = hashlib.md5(content).digest()
        return md5_hash[:4]  # Usa apenas os primeiros 4 bytes
    
    def serialize(self) -> bytes:
        """
        Serializa o pacote RDT 2.0.
        
        Returns:
            bytes: Pacote serializado no formato RDT 2.0
        """
        try:
            return struct.pack('!B', self.type) + self.checksum + self.data
        except struct.error as e:
            raise PacketError(f"Erro na serialização do pacote RDT 2.0: {e}")
    
    @classmethod
    def deserialize(cls, raw_data: bytes) -> 'RDT20Packet':
        """
        Reconstrói um pacote RDT 2.0 a partir de bytes.
        
        Args:
            raw_data: Dados brutos recebidos
            
        Returns:
            RDT20Packet: Instância do pacote reconstruído
        """
        if len(raw_data) < 5:  # 1 byte tipo + 4 bytes checksum
            raise PacketError("Dados insuficientes para pacote RDT 2.0")
        
        try:
            # Extrai componentes
            packet_type = struct.unpack('!B', raw_data[:1])[0]
            checksum = raw_data[1:5]
            data = raw_data[5:]
            
            # Valida tipo
            if packet_type not in cls.VALID_TYPES:
                raise PacketError(f"Tipo de pacote inválido: {packet_type}")
            
            return cls(packet_type, data, checksum)
            
        except struct.error as e:
            raise PacketError(f"Erro na desserialização RDT 2.0: {e}")
    
    def is_corrupted(self) -> bool:
        """
        Verifica se o pacote está corrompido comparando checksums.
        
        Returns:
            bool: True se corrompido, False caso contrário
        """
        expected_checksum = self._calculate_checksum()
        return self.checksum != expected_checksum
    
    def __str__(self) -> str:
        """Representação string do pacote RDT 2.0."""
        corrupted = " (CORRUPTED)" if self.is_corrupted() else ""
        return f"RDT20Packet(type={self.get_type_name()}, data_len={len(self.data)}{corrupted})"


class RDT21Packet(RDT20Packet):
    """
    Pacote RDT 2.1 com número de sequência para detecção de duplicatas.
    
    Formato: | Tipo (1 byte) | SeqNum (1 byte) | Checksum (4 bytes) | Dados (variável) |
    """
    
    def __init__(self, packet_type: int, seq_num: int = 0, data: bytes = b'', checksum: Optional[bytes] = None):
        """
        Inicializa um pacote RDT 2.1.
        
        Args:
            packet_type: Tipo do pacote
            seq_num: Número de sequência (0 ou 1)
            data: Dados do pacote
            checksum: Checksum pré-calculado (opcional)
        """
        if seq_num not in {0, 1}:
            raise PacketError(f"Número de sequência inválido: {seq_num}. Deve ser 0 ou 1")
        
        self.seq_num = seq_num
        super().__init__(packet_type, data, checksum)
    
    def _calculate_checksum(self) -> bytes:
        """
        Calcula checksum MD5 incluindo número de sequência.
        
        Returns:
            bytes: Primeiros 4 bytes do hash MD5
        """
        # Combina tipo, seq_num e dados para o checksum
        content = struct.pack('!BB', self.type, self.seq_num) + self.data
        md5_hash = hashlib.md5(content).digest()
        return md5_hash[:4]
    
    def serialize(self) -> bytes:
        """
        Serializa o pacote RDT 2.1.
        
        Returns:
            bytes: Pacote serializado no formato RDT 2.1
        """
        try:
            return struct.pack('!BB', self.type, self.seq_num) + self.checksum + self.data
        except struct.error as e:
            raise PacketError(f"Erro na serialização do pacote RDT 2.1: {e}")
    
    @classmethod
    def deserialize(cls, raw_data: bytes) -> 'RDT21Packet':
        """
        Reconstrói um pacote RDT 2.1 a partir de bytes.
        
        Args:
            raw_data: Dados brutos recebidos
            
        Returns:
            RDT21Packet: Instância do pacote reconstruído
        """
        if len(raw_data) < 6:  # 1 byte tipo + 1 byte seq + 4 bytes checksum
            raise PacketError("Dados insuficientes para pacote RDT 2.1")
        
        try:
            # Extrai componentes
            packet_type, seq_num = struct.unpack('!BB', raw_data[:2])
            checksum = raw_data[2:6]
            data = raw_data[6:]
            
            # Valida tipo e seq_num
            if packet_type not in cls.VALID_TYPES:
                raise PacketError(f"Tipo de pacote inválido: {packet_type}")
            
            if seq_num not in {0, 1}:
                raise PacketError(f"Número de sequência inválido: {seq_num}")
            
            return cls(packet_type, seq_num, data, checksum)
            
        except struct.error as e:
            raise PacketError(f"Erro na desserialização RDT 2.1: {e}")
    
    def __str__(self) -> str:
        """Representação string do pacote RDT 2.1."""
        corrupted = " (CORRUPTED)" if self.is_corrupted() else ""
        return f"RDT21Packet(type={self.get_type_name()}, seq={self.seq_num}, data_len={len(self.data)}{corrupted})"


# Alias para RDT 3.0 (usa mesmo formato do RDT 2.1)
RDT30Packet = RDT21Packet


class PipelinePacket(Packet):
    """
    Pacote para protocolos de pipelining com números de sequência de 32 bits.
    
    Formato: | Tipo (1 byte) | SeqNum (4 bytes) | Checksum (4 bytes) | Dados (variável) |
    
    Suporta números de sequência de 0 a 2^32-1 para longas transmissões de pipelining.
    """
    
    def __init__(self, packet_type: int, seq_num: int = 0, data: bytes = b'', 
                 checksum: Optional[bytes] = None):
        """
        Inicializa um pacote de pipelining.
        
        Args:
            packet_type: Tipo do pacote (DATA=0, ACK=1, NAK=2)
            seq_num: Número de sequência (0 a 2^32-1)
            data: Dados do pacote
            checksum: Checksum pré-calculado (opcional)
            
        Raises:
            PacketError: Se seq_num estiver fora do range válido
        """
        super().__init__(packet_type, data)
        
        # Validar range do número de sequência de 32 bits
        if not (0 <= seq_num <= 0xFFFFFFFF):
            raise PacketError(f"Número de sequência inválido: {seq_num}. Deve estar entre 0 e {0xFFFFFFFF}")
        
        self.seq_num = seq_num
        self.checksum = checksum or self._calculate_checksum()
    
    def _calculate_checksum(self) -> bytes:
        """
        Calcula checksum MD5 incluindo tipo, seq_num de 32 bits e dados.
        
        Returns:
            bytes: Primeiros 4 bytes do hash MD5
        """
        # Combina tipo (1 byte) + seq_num (4 bytes) + dados para o checksum
        content = struct.pack('!BI', self.type, self.seq_num) + self.data
        md5_hash = hashlib.md5(content).digest()
        return md5_hash[:4]  # Usa apenas os primeiros 4 bytes
    
    def serialize(self) -> bytes:
        """
        Serializa o pacote de pipelining.
        
        Formato: | Tipo (1B) | SeqNum (4B) | Checksum (4B) | Dados (variável) |
        
        Returns:
            bytes: Pacote serializado no formato de pipelining
            
        Raises:
            PacketError: Se houver erro na serialização
        """
        try:
            # Formato: tipo (1 byte) + seq_num (4 bytes) + checksum (4 bytes) + dados
            header = struct.pack('!BI', self.type, self.seq_num)
            return header + self.checksum + self.data
        except struct.error as e:
            raise PacketError(f"Erro na serialização do pacote de pipelining: {e}")
    
    @classmethod
    def deserialize(cls, raw_data: bytes) -> 'PipelinePacket':
        """
        Reconstrói um pacote de pipelining a partir de bytes.
        
        Args:
            raw_data: Dados brutos recebidos
            
        Returns:
            PipelinePacket: Instância do pacote reconstruído
            
        Raises:
            PacketError: Se os dados estiverem malformados
        """
        if len(raw_data) < 9:  # 1 byte tipo + 4 bytes seq_num + 4 bytes checksum
            raise PacketError("Dados insuficientes para pacote de pipelining")
        
        try:
            # Extrai componentes
            packet_type, seq_num = struct.unpack('!BI', raw_data[:5])
            checksum = raw_data[5:9]
            data = raw_data[9:]
            
            # Valida tipo
            if packet_type not in cls.VALID_TYPES:
                raise PacketError(f"Tipo de pacote inválido: {packet_type}")
            
            # Valida range do seq_num (já validado no construtor, mas verificação extra)
            if not (0 <= seq_num <= 0xFFFFFFFF):
                raise PacketError(f"Número de sequência fora do range: {seq_num}")
            
            return cls(packet_type, seq_num, data, checksum)
            
        except struct.error as e:
            raise PacketError(f"Erro na desserialização do pacote de pipelining: {e}")
    
    def is_corrupted(self) -> bool:
        """
        Verifica se o pacote está corrompido comparando checksums.
        
        Returns:
            bool: True se corrompido, False caso contrário
        """
        expected_checksum = self._calculate_checksum()
        return self.checksum != expected_checksum
    
    def is_valid_sequence_number(self) -> bool:
        """
        Verifica se o número de sequência está no range válido.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        return 0 <= self.seq_num <= 0xFFFFFFFF
    
    def get_sequence_number(self) -> int:
        """
        Retorna o número de sequência do pacote.
        
        Returns:
            int: Número de sequência
        """
        return self.seq_num
    
    def __str__(self) -> str:
        """Representação string do pacote de pipelining."""
        corrupted = " (CORRUPTED)" if self.is_corrupted() else ""
        return f"PipelinePacket(type={self.get_type_name()}, seq={self.seq_num}, data_len={len(self.data)}{corrupted})"
    
    def __repr__(self) -> str:
        """Representação detalhada do pacote de pipelining."""
        data_preview = self.data[:20] if len(self.data) <= 20 else self.data[:20] + b'...'
        return f"PipelinePacket(type={self.type}, seq_num={self.seq_num}, data={data_preview})"