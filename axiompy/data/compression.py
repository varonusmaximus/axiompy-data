"""
Data compression utilities for efficient storage and transfer.

Provides utilities to compress and decompress data using various formats
including gzip, bzip2, zstd, lz4, and snappy.
"""

import bz2
import gzip
from pathlib import Path
from typing import Optional, Union

from axiompy.loggers import LoggerFactory

from axiompy.data.types import CompressionFormat

logger = LoggerFactory.create_logger(__name__)


class DataCompressor:
    """
    Utility for compressing and decompressing data.

    Supports multiple compression formats with automatic format detection.
    """

    def __init__(self, default_format: CompressionFormat = CompressionFormat.GZIP):
        """
        Initialize the compressor.

        Args:
            default_format: Default compression format to use
        """
        self.default_format = default_format

    def compress(
        self, data: Union[bytes, str], format: Optional[CompressionFormat] = None, level: int = 6
    ) -> bytes:
        """
        Compress data using specified format.

        Args:
            data: Data to compress (bytes or string)
            format: Compression format (uses default if None)
            level: Compression level (1-9, higher = more compression)

        Returns:
            Compressed data as bytes
        """
        format = format or self.default_format

        if isinstance(data, str):
            data = data.encode("utf-8")

        logger.debug(f"Compressing {len(data)} bytes using {format.value}")

        if format == CompressionFormat.GZIP:
            compressed = gzip.compress(data, compresslevel=level)

        elif format == CompressionFormat.BZIP2:
            compressed = bz2.compress(data, compresslevel=level)

        elif format == CompressionFormat.ZSTD:
            try:
                import zstandard as zstd

                compressor = zstd.ZstdCompressor(level=level)
                compressed = compressor.compress(data)
            except ImportError:
                raise ImportError(
                    "zstandard is required for zstd compression. "
                    "Install with: pip install zstandard"
                )

        elif format == CompressionFormat.LZ4:
            try:
                import lz4.frame

                compressed = lz4.frame.compress(data, compression_level=level)
            except ImportError:
                raise ImportError(
                    "lz4 is required for lz4 compression. Install with: pip install lz4"
                )

        elif format == CompressionFormat.SNAPPY:
            try:
                import snappy

                compressed = snappy.compress(data)
            except ImportError:
                raise ImportError(
                    "python-snappy is required for snappy compression. "
                    "Install with: pip install python-snappy"
                )

        elif format == CompressionFormat.NONE:
            compressed = data

        else:
            raise ValueError(f"Unsupported compression format: {format}")

        compression_ratio = (1 - len(compressed) / len(data)) * 100
        logger.debug(f"Compressed to {len(compressed)} bytes ({compression_ratio:.1f}% reduction)")

        return compressed

    def decompress(self, data: bytes, format: Optional[CompressionFormat] = None) -> bytes:
        """
        Decompress data.

        Args:
            data: Compressed data
            format: Compression format (auto-detected if None)

        Returns:
            Decompressed data as bytes
        """
        if format is None:
            format = self._detect_format(data)

        logger.debug(f"Decompressing {len(data)} bytes using {format.value}")

        if format == CompressionFormat.GZIP:
            decompressed = gzip.decompress(data)

        elif format == CompressionFormat.BZIP2:
            decompressed = bz2.decompress(data)

        elif format == CompressionFormat.ZSTD:
            try:
                import zstandard as zstd

                decompressor = zstd.ZstdDecompressor()
                decompressed = decompressor.decompress(data)
            except ImportError:
                raise ImportError("zstandard is required. Install with: pip install zstandard")

        elif format == CompressionFormat.LZ4:
            try:
                import lz4.frame

                decompressed = lz4.frame.decompress(data)
            except ImportError:
                raise ImportError("lz4 is required. Install with: pip install lz4")

        elif format == CompressionFormat.SNAPPY:
            try:
                import snappy

                decompressed = snappy.decompress(data)
            except ImportError:
                raise ImportError(
                    "python-snappy is required. Install with: pip install python-snappy"
                )

        elif format == CompressionFormat.NONE:
            decompressed = data

        else:
            raise ValueError(f"Unsupported compression format: {format}")

        logger.debug(f"Decompressed to {len(decompressed)} bytes")

        return decompressed

    def compress_file(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        format: Optional[CompressionFormat] = None,
        level: int = 6,
        remove_source: bool = False,
    ) -> str:
        """
        Compress a file.

        Args:
            input_path: Path to input file
            output_path: Path to output file (auto-generated if None)
            format: Compression format
            level: Compression level
            remove_source: Whether to remove source file after compression

        Returns:
            Path to compressed file
        """
        format = format or self.default_format

        if output_path is None:
            output_path = f"{input_path}.{self._get_extension(format)}"

        logger.info(f"Compressing file: {input_path} → {output_path}")

        with open(input_path, "rb") as f:
            data = f.read()

        compressed = self.compress(data, format, level)

        with open(output_path, "wb") as f:
            f.write(compressed)

        if remove_source:
            Path(input_path).unlink()
            logger.info(f"Removed source file: {input_path}")

        return output_path

    def decompress_file(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        format: Optional[CompressionFormat] = None,
        remove_source: bool = False,
    ) -> str:
        """
        Decompress a file.

        Args:
            input_path: Path to compressed file
            output_path: Path to output file (auto-generated if None)
            format: Compression format (auto-detected if None)
            remove_source: Whether to remove compressed file after decompression

        Returns:
            Path to decompressed file
        """
        if output_path is None:
            # Remove compression extension
            path = Path(input_path)
            if path.suffix in [".gz", ".bz2", ".zst", ".lz4", ".snappy"]:
                output_path = str(path.with_suffix(""))
            else:
                output_path = f"{input_path}.decompressed"

        logger.info(f"Decompressing file: {input_path} → {output_path}")

        with open(input_path, "rb") as f:
            data = f.read()

        decompressed = self.decompress(data, format)

        with open(output_path, "wb") as f:
            f.write(decompressed)

        if remove_source:
            Path(input_path).unlink()
            logger.info(f"Removed source file: {input_path}")

        return output_path

    def compare_formats(self, data: Union[bytes, str], level: int = 6) -> dict:
        """
        Compare compression ratios across all formats.

        Args:
            data: Data to compress
            level: Compression level

        Returns:
            Dictionary with results for each format
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        original_size = len(data)
        results = {}

        for format in [
            CompressionFormat.GZIP,
            CompressionFormat.BZIP2,
            CompressionFormat.ZSTD,
            CompressionFormat.LZ4,
            CompressionFormat.SNAPPY,
        ]:
            try:
                compressed = self.compress(data, format, level)
                compressed_size = len(compressed)
                ratio = (1 - compressed_size / original_size) * 100

                results[format.value] = {
                    "compressed_size": compressed_size,
                    "compression_ratio": ratio,
                    "original_size": original_size,
                }
            except ImportError as e:
                results[format.value] = {"error": str(e)}

        return results

    def _detect_format(self, data: bytes) -> CompressionFormat:
        """Auto-detect compression format from magic bytes."""
        if data[:2] == b"\x1f\x8b":  # gzip magic number
            return CompressionFormat.GZIP
        elif data[:2] == b"BZ":  # bzip2 magic number
            return CompressionFormat.BZIP2
        elif data[:4] == b"\x28\xb5\x2f\xfd":  # zstd magic number
            return CompressionFormat.ZSTD
        elif data[:4] == b"\x04\x22\x4d\x18":  # lz4 magic number
            return CompressionFormat.LZ4
        else:
            logger.warning("Could not detect compression format, assuming GZIP")
            return CompressionFormat.GZIP

    def _get_extension(self, format: CompressionFormat) -> str:
        """Get file extension for compression format."""
        extensions = {
            CompressionFormat.GZIP: "gz",
            CompressionFormat.BZIP2: "bz2",
            CompressionFormat.ZSTD: "zst",
            CompressionFormat.LZ4: "lz4",
            CompressionFormat.SNAPPY: "snappy",
            CompressionFormat.NONE: "raw",
        }
        return extensions.get(format, "compressed")
