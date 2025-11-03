"""
ASCII art banner and about information for PhasorPoint CLI
"""

from .constants import CLI_COMMAND_PYTHON

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "unknown"


# ASCII Art Banner - Phasor diagram with sine wave
BANNER = r"""
    ____  __                            ____        _       __     ________    ____
   / __ \/ /_  ____ ________  _____    / __ \____  (_)___  / /_   / ____/ /   /  _/
  / /_/ / __ \/ __ `/ ___/ / / / __/  / /_/ / __ \/ / __ \/ __/  / /   / /    / /
 / ____/ / / / /_/ (__  ) /_/ / /    / ____/ /_/ / / / / / /_   / /___/ /____/ /
/_/   /_/ /_/\__,_/____/\__,_/_/    /_/    \____/_/_/ /_/\__|   \____/_____/___/

    ~∿~  PMU Data Extraction Tool  ~∿~
"""


def get_banner():
    """Get the ASCII art banner."""
    return BANNER


def get_version():
    """Get the current version."""
    return __version__


def get_about_text():
    """Get the full about text with version and author information."""
    return f"""{BANNER}
Version: {__version__}

A comprehensive command-line interface for extracting and
processing PMU (Phasor Measurement Unit) data from PhasorPoint databases.

Author:  Frederik Fast (Energinet)
Email:   ffb@energinet.dk
Repo:    https://github.com/energinet-ti/phasor-point-cli
License: Apache License 2.0

Features:
  • Flexible data extraction (minutes, hours, days, or custom ranges)
  • Automatic power calculations (S, P, Q) and quality validation
  • Multiple formats (Parquet, CSV)
  • Batch operations for multiple PMUs
  • Performance optimization (chunking, parallel processing, connection pooling)
  • Custom SQL queries

Quick Start:
  {CLI_COMMAND_PYTHON} setup              # Configure database connection
  {CLI_COMMAND_PYTHON} list-tables        # List available PMU tables
  {CLI_COMMAND_PYTHON} extract --pmu 45020 --hours 1 --output data.parquet

For help:
  {CLI_COMMAND_PYTHON} --help            # Show all commands
  {CLI_COMMAND_PYTHON} <command> --help  # Show command-specific help
"""


def print_banner():
    """Print the banner to console."""
    print(get_banner())


def print_about():
    """Print the full about information to console."""
    print(get_about_text())


# Pirate raccoon ASCII art (hidden easter egg)
PIRATE_RACCOON = r"""
    ╔════════════════════════════════════════════════════════════════╗
    ║                                                                ║
    ║   AHOY, MATEY! Ye found Captain Trash Panda!                   ║
    ║                                                                ║
    ║   Arrr! This be a hidden treasure in the code!                 ║
    ║   The legendary data pirate of the seven servers!              ║
    ║                                                                ║
    ║   Keep searchin' for more easter eggs, ye scurvy landlubber!   ║
    ║                                                                ║
    ║   ~∿~ Captain Trash Panda ~∿~                                  ║
    ║                                                                ║
    ╚════════════════════════════════════════════════════════════════╝

                    ⠀⠀⠀⠀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
                    ⠀⠀⠀⡌⡀⠉⠓⢤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⠄⡄⠀
                    ⠀⠀⠀⣇⢧⣶⡠⣬⣿⣶⣤⣤⣠⣴⣦⣤⣄⣀⠀⣀⣠⠞⢛⣡⣴⣾⣾⠀
                    ⠀⠀⠀⠹⡜⣿⣿⣯⠻⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣶⣩⣽⣿⡿⡽⠀
                    ⠀⠀⠀⠀⢺⠄⡻⠀⠐⢻⣿⣿⣿⣿⠻⢻⠿⢻⣿⢿⣿⣿⣿⣿⡿⡕⠁⠀
                    ⠀⠀⠀⠀⢨⡆⠁⠀⠀⠀⠀⣿⡟⠃⠀⠈⠀⠀⠈⠻⢻⣿⣿⠏⢠⠃⠀⠀
                    ⠀⠀⠀⠀⢨⠃⠀⣀⣰⡄⢰⡿⠁⢠⣆⣀⠀⠀⠀⠀⠈⠻⢧⠀⢇⠀⠀⠀
                    ⠀⠀⠀⠀⠞⠀⣼⣿⣾⡗⢸⠇⣰⣿⣿⣿⣿⣷⣤⠤⠀⢀⢈⠀⢸⡂⠀⠀
                    ⠀⠈⠐⠤⠆⢼⣿⣿⠋⠀⠀⠀⠀⠻⣿⣿⣿⣿⣿⣿⣿⣚⣟⠀⠀⢳⡄⠀
                    ⠀⠤⠤⢴⠧⣐⡺⢅⣤⣶⣶⡀⠀⠀⣻⣿⣿⠟⣻⣿⣿⡿⠷⢶⡀⠀⢻⠂
                    ⠀⢈⡩⠿⢷⠒⠒⣺⣿⣿⣿⣧⣶⠠⠈⢨⣻⣿⡿⣿⣿⣿⡤⡄⢩⣬⡿⠆
                    ⠈⠀⠀⠀⢚⡃⢀⣿⣿⣛⣿⣛⣻⣯⣽⣿⣿⣿⣿⣮⣿⣿⣿⣶⣦⣽⡍⠀
                    ⠀⠀⠀⠀⢸⣅⢹⣿⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣾⠻⠃⠀
                    ⠀⠀⠀⠀⠈⠋⠘⠻⠋⢿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠟⠛⠛⠁⠁⠀⠀⠀⠀
                    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

"""


def get_pirate_raccoon() -> str:
    """
    Get the hidden pirate raccoon ASCII art.

    Returns:
        str: ASCII art of a pirate raccoon
    """
    return PIRATE_RACCOON


def print_pirate_raccoon():
    """Print the hidden pirate raccoon easter egg."""
    print(get_pirate_raccoon())
