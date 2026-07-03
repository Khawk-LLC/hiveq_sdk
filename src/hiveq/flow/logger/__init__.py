import logging
import sys

from datetime import datetime, timezone

import colorlog

# Module-level flag to control logo display
_logo_displayed = False


class HiveQLogFormatter(logging.Formatter):
    def __init__(self, clock=None):
        super().__init__()
        self.clock = clock

    def formatTime(self, record, datefmt=None):
        if self.clock is not None:
            try:
                # Use the engine clock timestamp (simulation time during backtest)
                timestamp_ns = self.clock.timestamp_ns()
                # Check if timestamp is valid (not epoch/1970 or too far in future)
                if timestamp_ns > 1_000_000_000_000_000_000:  # Year 2001+
                    dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
                else:
                    # Invalid timestamp, use real time
                    dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
            except:
                # Clock error, use real time
                dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        else:
            # Fallback to real time
            dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # 6-digit microseconds


def logger(show_logo: bool = True) -> logging.Logger:
    """
    Get or create the HiveQ logger with colored formatting.

    Args:
        show_logo: Whether to display the HiveQ logo if initializing handlers.
                   Defaults to True. Set to False when reinitializing after reset.

    Returns:
        The configured logger instance.
    """
    global _logo_displayed

    hiveq_logger = logging.getLogger("hiveq.flow")
    hiveq_logger.propagate = False  # don't send logs to OMS / root handlers

    if not hiveq_logger.handlers:
        # Determine default log level based on execution environment
        # Deploy mode (running in executor container) defaults to CRITICAL to minimize log noise
        # Local mode defaults to INFO for better visibility during development
        import os
        is_deploy_mode = os.environ.get('TASK_ID') is not None
        default_level = logging.CRITICAL if is_deploy_mode else logging.INFO

        hiveq_logger.setLevel(default_level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(default_level)

        # Create colored formatter that also uses clock time
        class ColoredHiveqFormatter(colorlog.ColoredFormatter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.clock = None  # Will be set later by set_hiveq_logger_clock()

            def formatTime(self, record, datefmt=None):
                if self.clock is not None:
                    try:
                        timestamp_ns = self.clock.timestamp_ns()
                        # Check if timestamp is valid (not epoch/1970 or too far in future)
                        if timestamp_ns > 1_000_000_000_000_000_000:  # Year 2001+
                            dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
                        else:
                            # Invalid timestamp, use real time
                            dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                    except:
                        # Clock error, use real time
                        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                else:
                    dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d %H:%M:%S")

        formatter = ColoredHiveqFormatter(
            "%(log_color)s%(asctime)s [%(levelname)s] [hiveq] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            }
        )
        ch.setFormatter(formatter)
        hiveq_logger.addHandler(ch)

        # Disable findCaller() frame-walking.  The log format does not use
        # %(filename)s or %(lineno)d, so walking f_back is wasted work.
        # More critically, when callbacks arrive from PySigma C++ code, the
        # Python frame chain crosses C++/Python boundaries where f_back
        # pointers are invalid.  Walking those frames corrupts interpreter
        # state and causes delayed segfaults (e.g. during DataFrame.to_string()).
        hiveq_logger.findCaller = lambda *_args, **_kw: ("(unknown)", 0, "(unknown)", None)

        # Print HiveQ beehive logo only if requested and not already displayed
        # Also check environment variable to allow suppression in deployed environments
        import os
        suppress_logo = os.environ.get('HIVEQ_SUPPRESS_LOGO', 'false').lower() == 'true'
        if show_logo and not _logo_displayed and not suppress_logo:
            # Display logo using logger info messages
            logo_lines = [
                "==========================================",
                "      ___           ___       ",
                "     /   \\         /   \\    ",
                "    /     \\_______/     \\   ",
                "    \\      /     \\      /   ",
                "     \\____/  HIVE \\____/    ",
                "     /    \\   Q   /    \\    ",
                "    /      \\_____/      \\   ",
                "    \\       /   \\       /   ",
                "     \\_____/     \\_____/    ",
                "                              ",
                "==========================================",
            ]

            # Log each line of the logo
            for line in logo_lines:
                hiveq_logger.info(line)

            # Version of whichever distribution provides this package. Try the
            # full engine ("hiveq-flow") first, then the thin client
            # ("hiveq-sdk") explicitly -- both may be present. Only fall back to
            # "whichever distribution shares the `hiveq` namespace" as a last
            # resort: other unrelated packages (HiveQDataDriver, hiveq-orchestrator)
            # also install files under `hiveq/`, so grabbing the first one blindly
            # can print an unrelated package's version (observed: HiveQDataDriver's
            # "1.0.0" shown instead of hiveq-sdk's own version when both share an
            # environment).
            flow_version = None
            try:
                from importlib.metadata import version, PackageNotFoundError, packages_distributions
                for _known in ("hiveq-flow", "hiveq-sdk"):
                    try:
                        flow_version = version(_known)
                        break
                    except PackageNotFoundError:
                        continue
                if flow_version is None:
                    for _dist in packages_distributions().get("hiveq", []):
                        try:
                            flow_version = version(_dist)
                            break
                        except PackageNotFoundError:
                            continue
            except Exception:
                pass
            hiveq_logger.info(f"         HiveQ Flow v{flow_version or '0.1.0'}               ")
            hiveq_logger.info("==========================================")

            _logo_displayed = True

    return hiveq_logger


def set_logger_clock(clock):
    """
    Set the clock for the HiveQ logger formatters.

    This allows setting the clock on an already-existing logger,
    regardless of when get_hiveq_logger() was first called.

    Args:
        clock: engine clock instance for simulation time
    """
    hiveq_logger = logging.getLogger("hiveq.flow")

    # Update all existing handlers' formatters with the clock
    for handler in hiveq_logger.handlers:
        formatter = handler.formatter
        if hasattr(formatter, 'clock'):
            formatter.clock = clock


def clear_logger_clock():
    """Clear the clock reference from all logger formatters.

    Must be called BEFORE destroying the C++ engine, since the clock
    is a C++ object that will be freed by engine.destroy().
    """
    hiveq_logger = logging.getLogger("hiveq.flow")
    for handler in hiveq_logger.handlers:
        formatter = handler.formatter
        if hasattr(formatter, 'clock'):
            formatter.clock = None


def reset_logger():
    """
    Reset the hiveq logger by removing all handlers and reinitializing.
    This allows the logo to be displayed again on the next initialization
    and ensures colored handlers are restored for subsequent runs.
    """
    global _logo_displayed

    hiveq_logger = logging.getLogger("hiveq.flow")
    # Remove all existing handlers
    for handler in hiveq_logger.handlers[:]:
        handler.close()
        hiveq_logger.removeHandler(handler)
    hiveq_logger.handlers.clear()

    # Reset logo flag so it displays on next run
    _logo_displayed = False

    # Reinitialize the logger with colored handlers (but no logo yet)
    logger(show_logo=False)


def set_log_level(level: str):
    """
    Set the HiveQ logger level dynamically.

    Args:
        level: Log level as string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    hiveq_logger = logging.getLogger("hiveq.flow")
    log_level = getattr(logging, level.upper(), logging.INFO)
    hiveq_logger.setLevel(log_level)

    # Also update all handlers
    for handler in hiveq_logger.handlers:
        handler.setLevel(log_level)


