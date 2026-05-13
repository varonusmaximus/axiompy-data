"""
Unit tests for axiompy.data.compression module.

Tests DataCompressor for various compression formats.
"""

from pathlib import Path

import pytest

from axiompy.data.compression import DataCompressor
from axiompy.data.types import CompressionFormat


class TestDataCompressor:
    """Test DataCompressor class."""

    @pytest.fixture
    def compressor(self):
        """Create DataCompressor instance."""
        return DataCompressor()

    @pytest.fixture
    def sample_data(self):
        """Create sample data for compression."""
        return b"Hello, this is sample data to compress! " * 100

    def test_compress_gzip(self, compressor, sample_data):
        """Test GZIP compression."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.GZIP)

        assert len(compressed) < len(sample_data)
        assert compressed[:2] == b"\x1f\x8b"  # GZIP magic number

    def test_compress_bzip2(self, compressor, sample_data):
        """Test BZIP2 compression."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.BZIP2)

        assert len(compressed) < len(sample_data)
        assert compressed[:2] == b"BZ"  # BZIP2 magic number

    def test_compress_string_input(self, compressor):
        """Test compressing string input."""
        text = "Hello, World!" * 100
        compressed = compressor.compress(text, format=CompressionFormat.GZIP)

        assert isinstance(compressed, bytes)
        assert len(compressed) < len(text.encode("utf-8"))

    def test_compress_compression_levels(self, compressor, sample_data):
        """Test different compression levels."""
        compressed_low = compressor.compress(sample_data, format=CompressionFormat.GZIP, level=1)
        compressed_high = compressor.compress(sample_data, format=CompressionFormat.GZIP, level=9)

        # Higher compression should produce smaller output (usually)
        # But we just verify both work
        assert len(compressed_low) > 0
        assert len(compressed_high) > 0

    def test_decompress_gzip(self, compressor, sample_data):
        """Test GZIP decompression."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.GZIP)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.GZIP)

        assert decompressed == sample_data

    def test_decompress_bzip2(self, compressor, sample_data):
        """Test BZIP2 decompression."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.BZIP2)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.BZIP2)

        assert decompressed == sample_data

    def test_decompress_auto_detect_gzip(self, compressor, sample_data):
        """Test auto-detection of GZIP format."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.GZIP)
        decompressed = compressor.decompress(compressed)  # No format specified

        assert decompressed == sample_data

    def test_decompress_auto_detect_bzip2(self, compressor, sample_data):
        """Test auto-detection of BZIP2 format."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.BZIP2)
        decompressed = compressor.decompress(compressed)  # No format specified

        assert decompressed == sample_data

    def test_compress_none_format(self, compressor, sample_data):
        """Test 'none' format (no compression)."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.NONE)

        assert compressed == sample_data

    def test_compress_file(self, compressor, tmp_path):
        """Test compressing a file."""
        # Create test file
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        # Compress
        output_file = compressor.compress_file(str(input_file), format=CompressionFormat.GZIP)

        assert Path(output_file).exists()
        assert Path(output_file).stat().st_size < input_file.stat().st_size
        assert output_file.endswith(".gz")

    def test_compress_file_custom_output(self, compressor, tmp_path):
        """Test compressing file with custom output path."""
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        output_file = tmp_path / "compressed.gz"

        result = compressor.compress_file(
            str(input_file), output_path=str(output_file), format=CompressionFormat.GZIP
        )

        assert result == str(output_file)
        assert output_file.exists()

    def test_compress_file_remove_source(self, compressor, tmp_path):
        """Test removing source file after compression."""
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        compressor.compress_file(str(input_file), format=CompressionFormat.GZIP, remove_source=True)

        assert not input_file.exists()

    def test_decompress_file(self, compressor, tmp_path):
        """Test decompressing a file."""
        # Create and compress a file
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        compressed_file = compressor.compress_file(str(input_file))

        # Decompress
        output_file = compressor.decompress_file(compressed_file)

        assert Path(output_file).exists()
        assert Path(output_file).read_text() == input_file.read_text()

    def test_decompress_file_custom_output(self, compressor, tmp_path):
        """Test decompressing file with custom output path."""
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        compressed_file = compressor.compress_file(str(input_file))
        output_file = tmp_path / "decompressed.txt"

        result = compressor.decompress_file(compressed_file, output_path=str(output_file))

        assert result == str(output_file)
        assert output_file.exists()

    def test_decompress_file_remove_source(self, compressor, tmp_path):
        """Test removing compressed file after decompression."""
        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        compressed_file = compressor.compress_file(str(input_file))
        compressed_path = Path(compressed_file)

        compressor.decompress_file(compressed_file, remove_source=True)

        assert not compressed_path.exists()

    def test_compare_formats(self, compressor, sample_data):
        """Test comparing compression formats."""
        results = compressor.compare_formats(sample_data)

        assert "gzip" in results
        assert "bzip2" in results

        # Check that results contain expected keys
        for format_name, stats in results.items():
            if "error" not in stats:
                assert "compressed_size" in stats
                assert "compression_ratio" in stats
                assert "original_size" in stats
                assert stats["original_size"] == len(sample_data)

    def test_compression_roundtrip_gzip(self, compressor, sample_data):
        """Test complete compression/decompression roundtrip with GZIP."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.GZIP, level=6)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.GZIP)

        assert decompressed == sample_data

    def test_compression_roundtrip_bzip2(self, compressor, sample_data):
        """Test complete compression/decompression roundtrip with BZIP2."""
        compressed = compressor.compress(sample_data, format=CompressionFormat.BZIP2, level=6)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.BZIP2)

        assert decompressed == sample_data

    def test_detect_format_gzip(self, compressor):
        """Test format detection for GZIP."""
        data = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00test"
        detected = compressor._detect_format(data)

        assert detected == CompressionFormat.GZIP

    def test_detect_format_bzip2(self, compressor):
        """Test format detection for BZIP2."""
        data = b"BZh9test"
        detected = compressor._detect_format(data)

        assert detected == CompressionFormat.BZIP2

    def test_get_extension(self, compressor):
        """Test getting file extension for format."""
        assert compressor._get_extension(CompressionFormat.GZIP) == "gz"
        assert compressor._get_extension(CompressionFormat.BZIP2) == "bz2"
        assert compressor._get_extension(CompressionFormat.ZSTD) == "zst"
        assert compressor._get_extension(CompressionFormat.LZ4) == "lz4"
        assert compressor._get_extension(CompressionFormat.SNAPPY) == "snappy"

    def test_compress_unsupported_format(self, compressor):
        """Test compression with unsupported format."""
        data = b"Test data"

        # Create a mock unsupported format
        class UnsupportedFormat:
            value = "unsupported"

        with pytest.raises(ValueError, match="Unsupported compression format"):
            compressor.compress(data, format=UnsupportedFormat())

    def test_decompress_unsupported_format(self, compressor):
        """Test decompression with unsupported format."""
        data = b"Test data"

        # Create a mock unsupported format
        class UnsupportedFormat:
            value = "unsupported"

        with pytest.raises(ValueError):
            compressor.decompress(data, format=UnsupportedFormat())

    def test_compress_with_default_format(self, compressor, sample_data):
        """Test compression using default format."""
        # Create compressor with default format
        comp = DataCompressor(default_format=CompressionFormat.BZIP2)

        # Compress without specifying format
        compressed = comp.compress(sample_data)

        assert len(compressed) < len(sample_data)
        assert compressed[:2] == b"BZ"

    def test_compress_none_format_bytes(self, compressor):
        """Test NONE format with bytes."""
        data = b"Test data"
        compressed = compressor.compress(data, format=CompressionFormat.NONE)

        assert compressed == data

    def test_decompress_none_format_bytes(self, compressor):
        """Test decompressing NONE format bytes."""
        data = b"Test data"
        compressed = compressor.compress(data, format=CompressionFormat.NONE)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.NONE)

        assert decompressed == data

    def test_autodetect_unsupported_format(self, compressor):
        """Test auto-detection with unsupported data."""
        # Data that doesn't match any known format
        data = b"\xff\xfe\xfd\xfc"

        # Should either raise ValueError or gzip.BadGzipFile when format not detected
        with pytest.raises((ValueError, Exception)):
            compressor.decompress(data)

    def test_compress_file_with_zstd(self, compressor, tmp_path):
        """Test compressing file with zstd format if available."""
        pytest.importorskip("zstandard")

        input_file = tmp_path / "test.txt"
        input_file.write_text("Hello, World! " * 100)

        output_file = compressor.compress_file(str(input_file), format=CompressionFormat.ZSTD)

        assert Path(output_file).exists()
        assert output_file.endswith(".zst")

    def test_decompress_file_with_zstd(self, compressor, tmp_path):
        """Test decompressing zstd file if available."""
        pytest.importorskip("zstandard")

        input_file = tmp_path / "test.txt"
        original_content = "Hello, World! " * 100
        input_file.write_text(original_content)

        compressed_file = compressor.compress_file(str(input_file), format=CompressionFormat.ZSTD)

        output_file = compressor.decompress_file(compressed_file)

        decompressed_content = Path(output_file).read_text()
        assert decompressed_content == original_content


class TestCompressionIntegration:
    """Integration tests for compression workflows."""

    def test_compress_large_text_file(self, tmp_path):
        """Test compressing a large text file."""
        compressor = DataCompressor()

        # Create a large text file
        input_file = tmp_path / "large.txt"
        input_file.write_text("Lorem ipsum dolor sit amet. " * 10000)

        original_size = input_file.stat().st_size

        # Compress with different formats
        gzip_file = compressor.compress_file(str(input_file), format=CompressionFormat.GZIP)
        bzip2_file = compressor.compress_file(str(input_file), format=CompressionFormat.BZIP2)

        gzip_size = Path(gzip_file).stat().st_size
        bzip2_size = Path(bzip2_file).stat().st_size

        # Both should be significantly smaller
        assert gzip_size < original_size * 0.5
        assert bzip2_size < original_size * 0.5

    def test_compress_binary_data(self):
        """Test compressing binary data."""
        compressor = DataCompressor()

        # Create binary data
        binary_data = bytes(range(256)) * 100

        compressed = compressor.compress(binary_data, format=CompressionFormat.GZIP)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.GZIP)

        assert decompressed == binary_data

    def test_compression_with_different_data_types(self):
        """Test compression works with different types of data."""
        compressor = DataCompressor()

        # Highly repetitive data (compresses well)
        repetitive = b"A" * 10000
        compressed_rep = compressor.compress(repetitive, format=CompressionFormat.GZIP)
        ratio_rep = (1 - len(compressed_rep) / len(repetitive)) * 100

        # Random-like data (compresses poorly)
        random_like = bytes(range(256)) * 40
        compressed_rand = compressor.compress(random_like, format=CompressionFormat.GZIP)
        ratio_rand = (1 - len(compressed_rand) / len(random_like)) * 100

        # Repetitive should compress much better
        assert ratio_rep > ratio_rand


# Tests that require optional compression libraries
class TestOptionalCompressionFormats:
    """Tests for compression formats that require additional libraries."""

    def test_zstd_compression(self):
        """Test ZSTD compression if available."""
        pytest.importorskip("zstandard")

        compressor = DataCompressor()
        data = b"Hello, World! " * 100

        compressed = compressor.compress(data, format=CompressionFormat.ZSTD)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.ZSTD)

        assert decompressed == data
        assert len(compressed) < len(data)

    def test_lz4_compression(self):
        """Test LZ4 compression if available."""
        pytest.importorskip("lz4")

        compressor = DataCompressor()
        data = b"Hello, World! " * 100

        compressed = compressor.compress(data, format=CompressionFormat.LZ4)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.LZ4)

        assert decompressed == data
        assert len(compressed) < len(data)

    def test_snappy_compression(self):
        """Test Snappy compression if available."""
        pytest.importorskip("snappy")

        compressor = DataCompressor()
        data = b"Hello, World! " * 100

        compressed = compressor.compress(data, format=CompressionFormat.SNAPPY)
        decompressed = compressor.decompress(compressed, format=CompressionFormat.SNAPPY)

        assert decompressed == data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
