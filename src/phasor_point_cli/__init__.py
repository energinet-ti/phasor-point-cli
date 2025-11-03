"""
PhasorPoint CLI Package
A command-line interface for exploring and extracting data from PhasorPoint database.

This package provides an object-oriented interface for working with PhasorPoint PMU data.
"""

from .argument_parser import CLIArgumentParser
from .chunk_strategy import ChunkStrategy
from .cli import PhasorPointCLI, main, setup_logging
from .command_router import CommandRouter
from .config import ConfigurationManager
from .connection_manager import ConnectionManager
from .connection_pool import JDBCConnectionPool
from .constants import CLI_COMMAND_PYTHON, CLI_COMMAND_SCRIPT, CONFIG_DIR_NAME
from .data_extractor import DataExtractor
from .data_file_writer import DataFileWriter
from .data_processor import DataProcessor
from .data_validator import DataValidator
from .date_utils import DateRangeCalculator
from .extraction_manager import ExtractionManager
from .file_utils import FileUtils
from .models import (
    BatchExtractionResult,
    ChunkResult,
    DataQualityThresholds,
    DateRange,
    ExtractionRequest,
    ExtractionResult,
    PhasorColumnMap,
    PMUInfo,
    QueryResult,
    TableDiscoveryResult,
    TableInfo,
    TableStatistics,
    ValidationCheck,
    ValidationResult,
    WriteResult,
)
from .power_calculator import PowerCalculator
from .query_executor import QueryExecutor
from .table_manager import TableManager

# Version is managed by setuptools-scm
try:
    from ._version import version as __version__
except ImportError:
    # Fallback for development installations without build
    __version__ = "0.0.0.dev0+unknown"
__all__ = [
    # Main CLI
    "PhasorPointCLI",
    "main",
    "setup_logging",
    # Presentation Layer
    "CLIArgumentParser",
    "CommandRouter",
    "DateRangeCalculator",
    # Configuration Management
    "ConfigurationManager",
    # Connection Management
    "ConnectionManager",
    "JDBCConnectionPool",
    # Business Logic Layer
    "TableManager",
    "ExtractionManager",
    "QueryExecutor",
    # Data Layer
    "ChunkStrategy",
    "DataExtractor",
    "DataProcessor",
    "DataValidator",
    "PowerCalculator",
    # Utility Classes
    "FileUtils",
    "DataFileWriter",
    # Constants
    "CLI_COMMAND_PYTHON",
    "CLI_COMMAND_SCRIPT",
    "CONFIG_DIR_NAME",
    # Data Models
    "ExtractionRequest",
    "ExtractionResult",
    "BatchExtractionResult",
    "ChunkResult",
    "DateRange",
    "QueryResult",
    "WriteResult",
    "PMUInfo",
    "DataQualityThresholds",
    "PhasorColumnMap",
    "TableDiscoveryResult",
    "TableInfo",
    "TableStatistics",
    "ValidationCheck",
    "ValidationResult",
]
