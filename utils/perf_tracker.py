import time
import os
import sys
import threading
import inspect
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from contextlib import contextmanager
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from functools import wraps
import re

# Constants
PERF_TRACKING = os.getenv('PERF_TRACKING', 'false').lower() == 'true'
PERF_TRACKING = False  # Override for testing

# Check production mode from environment variable
IS_PRODUCTION = os.getenv('IS_PRODUCTION', 'false').lower() == 'true'

# Display constants
SPINNER_CHARS = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
ORANGE = '\033[38;5;208m'
RESET = '\033[0m'
BLOCK_FULL = '█'
BLOCK_EMPTY = '░'
CHECKMARK = '✓'
MS_PER_BLOCK = 20  # ms per progress bar block
MAX_BLOCKS = 50    # maximum number of blocks in progress bar

# Print status on module load
if PERF_TRACKING and not IS_PRODUCTION:
    print(f"{ORANGE}[PERF] Performance tracking enabled. Output will be logged to /logs{RESET}")
else:
    print(f"{ORANGE}[PERF] Performance tracking disabled.{RESET}")

@dataclass
class TableMetrics:
    """Tracks detailed metrics for a single table"""
    name: str
    total_rows: int = 0
    rows_read: int = 0
    columns_read: Set[str] = field(default_factory=set)
    cells_read: int = 0
    last_filtered_count: int = 0
    operations: List[str] = field(default_factory=list)

@dataclass
class QueryStep:
    """Tracks a single step in the query execution"""
    step_number: int
    file: str
    function: str
    description: str
    start_time: float
    duration_ms: float = 0
    tables: Dict[str, TableMetrics] = field(default_factory=dict)
    systems_remaining: int = 0
    stations_remaining: int = 0
    signals_remaining: int = 0
    commodities_remaining: int = 0
    query_text: str = ""
    context: str = ""

class ProgressBar:
    """Visual progress display"""
    def __init__(self):
        self.current_ms: float = 0
        self.blocks = MAX_BLOCKS
        
    def update(self, elapsed_ms: float) -> str:
        """Update progress bar with current time"""
        self.current_ms = elapsed_ms
        blocks = min(int(elapsed_ms / MS_PER_BLOCK), self.blocks)
        return f"[{BLOCK_FULL * blocks}{BLOCK_EMPTY * (self.blocks - blocks)}] {elapsed_ms:.1f}ms"

class Spinner:
    """Animated terminal spinner"""
    def __init__(self):
        self.chars = SPINNER_CHARS
        self.index = 0
        self.last_update = 0
        self.is_running = False
        self._stop = False
        self._thread = None
        
    def _spin(self, display_callback):
        """Spin animation thread"""
        while not self._stop and self.is_running:
            if time.time() - self.last_update > 0.1:
                self.index = (self.index + 1) % len(self.chars)
                display_callback(self.chars[self.index])
                self.last_update = time.time()
            time.sleep(0.05)
            
    def start(self, display_callback):
        """Start spinner animation"""
        self.is_running = True
        self._stop = False
        self._thread = threading.Thread(
            target=self._spin,
            args=(display_callback,),
            daemon=True
        )
        self._thread.start()
        
    def stop(self):
        """Stop spinner animation"""
        self._stop = True
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=0.2)
        return CHECKMARK

class QueryTracker:
    """Tracks detailed query execution metrics"""
    def __init__(self):
        self.reset()  # Call reset to initialize everything
        
    def reset(self):
        """Reset tracker state for a new search"""
        self.start_time = time.time()
        self.steps = []
        self.step_counter = 0
        self.current_step = None
        
        # Initialize table_totals with safe defaults
        self.table_totals = {
            'systems': 22283,
            'stations': 268421,
            'mineral_signals': 138925,
            'station_commodities': 8005731,
            'station_commodities_mapped': 8005731,
            'commodity_types': 50
        }
        
        # Create logs directory in project root only if not in production
        if not IS_PRODUCTION:
            self.log_dir = 'logs'
            os.makedirs(self.log_dir, exist_ok=True)
            
            # Set log file path
            self.log_file = os.path.join(self.log_dir, f"perf_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            print(f"{ORANGE}[PERF] Logging performance data to: {self.log_file}{RESET}")
        
        self.progress = ProgressBar()
        self.spinner = Spinner()
        self._stop = False
        self.last_update = 0
        self.last_status_lines = 0
        
    def _init_table_counts(self, connection):
        """Initialize total row counts for main tables"""
        try:
            cur = connection.cursor()
            
            # Initialize with safe defaults first
            self.table_totals = {
                'systems': 22283,
                'stations': 268421,
                'mineral_signals': 138925,
                'station_commodities': 8005731,
                'station_commodities_mapped': 8005731,
                'commodity_types': 50
            }
            
            # Then try to get actual counts
            for table in self.table_totals.keys():
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    result = cur.fetchone()
                    if result and result[0] > 0:  # Only update if we get a positive count
                        self.table_totals[table] = result[0]
                except Exception as e:
                    print(f"{ORANGE}[PERF] Failed to get count for table {table}: {e}{RESET}")
                    # Keep the safe default of 1
                    
            cur.close()
        except Exception as e:
            print(f"{ORANGE}[PERF] Failed to get table counts: {e}{RESET}")
            print(f"{ORANGE}[PERF] Using safe default table counts.{RESET}")
            
    def update_context(self, file: str, function: str, context: str):
        """Update current execution context and display it"""
        if self.current_step:
            self.current_step.file = file
            self.current_step.function = function
            self.current_step.context = context
            # Show the update
            if self.spinner and self.spinner.chars:
                self._update_display(self.spinner.chars[self.spinner.index])
            
    def start_step(self, description: str):
        """Start tracking a new query step"""
        self.step_counter += 1  # Always increment counter when starting a new step
        self.current_step = QueryStep(
            step_number=self.step_counter,  # Use the counter
            file=inspect.currentframe().f_back.f_code.co_filename,
            function=inspect.currentframe().f_back.f_code.co_name,
            description=description,
            start_time=time.time()
        )
        
        # Initialize with previous step's counts or total counts
        if self.steps and self.steps[-1]:
            prev_step = self.steps[-1]
            self.current_step.systems_remaining = prev_step.systems_remaining
            self.current_step.stations_remaining = prev_step.stations_remaining
            self.current_step.signals_remaining = prev_step.signals_remaining
            self.current_step.commodities_remaining = prev_step.commodities_remaining
        else:
            self.current_step.systems_remaining = self.table_totals['systems']
            self.current_step.stations_remaining = self.table_totals['stations']
            self.current_step.signals_remaining = self.table_totals['mineral_signals']
            self.current_step.commodities_remaining = self.table_totals['station_commodities']
        
        # Start spinner for this step
        self.spinner.start(self._update_display)
        
    def end_step(self):
        """Complete current query step"""
        if self.current_step:
            # Stop spinner first
            self._stop = True
            self.spinner.stop()
            
            self.current_step.duration_ms = (time.time() - self.current_step.start_time) * 1000
            self.steps.append(self.current_step)
            
            # Clear current line
            print('\r\033[K', end='')
            
            # Show final step status with checkmark
            context = f" [{self.current_step.context}]" if self.current_step.context else ""
            final_status = f"{ORANGE}[{len(self.steps):02d}] [{os.path.basename(self.current_step.file)}] [{self.current_step.function}]{context} [{CHECKMARK}] {self.current_step.description} [{self.current_step.duration_ms:.1f}ms]"
            
            # Add remaining counts
            counts = []
            if self.current_step.systems_remaining:
                systems_total = max(1, self.table_totals['systems'])
                percent = (self.current_step.systems_remaining / systems_total * 100)
                counts.append(f"Systems: {self.current_step.systems_remaining:,} ({percent:.1f}%)")
                
            if self.current_step.stations_remaining:
                stations_total = max(1, self.table_totals['stations'])
                percent = (self.current_step.stations_remaining / stations_total * 100)
                counts.append(f"Stations: {self.current_step.stations_remaining:,} ({percent:.1f}%)")
                
            if self.current_step.signals_remaining:
                signals_total = max(1, self.table_totals['mineral_signals'])
                percent = (self.current_step.signals_remaining / signals_total * 100)
                counts.append(f"Signals: {self.current_step.signals_remaining:,} ({percent:.1f}%)")
                
            if self.current_step.commodities_remaining:
                commodities_total = max(1, self.table_totals['station_commodities'])
                percent = (self.current_step.commodities_remaining / commodities_total * 100)
                counts.append(f"Commodities: {self.current_step.commodities_remaining:,} ({percent:.1f}%)")
                
            if counts:
                final_status += f" | {' | '.join(counts)}"
                
            print(final_status + RESET)
            
            # Show operations summary
            if self.current_step.tables:
                print("\nOperations:")
                for table_name, metrics in self.current_step.tables.items():
                    total_rows = max(1, metrics.total_rows)  # Avoid division by zero
                    print(f"  {table_name}:")
                    print(f"    Rows: {metrics.rows_read:,} of {total_rows:,} ({metrics.rows_read/total_rows*100:.1f}%)")
                    print(f"    Columns: {', '.join(sorted(metrics.columns_read))}")
                    if metrics.operations:
                        print(f"    Actions: {', '.join(metrics.operations)}")
                print()  # Extra newline after operations
            
            # Write to log
            self._write_step_log()
            
            # Show intermediate summary if this was a major step
            if (self.current_step.description and 
                ("step" in self.current_step.description.lower() or
                 "search" in self.current_step.description.lower())):
                print(f"\n{ORANGE}Intermediate Summary:{RESET}")
                print(f"Steps completed: {len(self.steps)}")
                print(f"Total duration so far: {sum(s.duration_ms for s in self.steps):.1f}ms\n")
            
            self.current_step = None
            
    def track_table_access(self, table: str, columns: List[str], rows_read: int):
        """Track table access metrics"""
        if not self.current_step:
            return
            
        if table not in self.current_step.tables:
            total_rows = self.table_totals.get(table, 0)
            if total_rows == 0:  # If we don't have a count, try to get it
                try:
                    cur = self._connection.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    result = cur.fetchone()
                    if result:
                        total_rows = result[0]
                    cur.close()
                except:
                    total_rows = 1  # Fallback to avoid division by zero
                    
            self.current_step.tables[table] = TableMetrics(
                name=table,
                total_rows=total_rows
            )
            
        metrics = self.current_step.tables[table]
        metrics.rows_read += rows_read
        metrics.columns_read.update(columns)
        metrics.cells_read += rows_read * len(columns)
        metrics.last_filtered_count = rows_read
        
    def update_remaining_counts(self, systems: int = None, stations: int = None, 
                              signals: int = None, commodities: int = None):
        """Update counts of remaining items after filtering"""
        if not self.current_step:
            return
            
        # Only update counts that are explicitly provided (not None)
        if systems is not None:
            self.current_step.systems_remaining = min(systems, self.current_step.systems_remaining)
        if stations is not None:
            self.current_step.stations_remaining = min(stations, self.current_step.stations_remaining)
        if signals is not None:
            self.current_step.signals_remaining = min(signals, self.current_step.signals_remaining)
        if commodities is not None:
            self.current_step.commodities_remaining = min(commodities, self.current_step.commodities_remaining)
        
    def _update_display(self, spinner_char: str):
        """Update terminal display with history"""
        if not sys.stdout.isatty() or self._stop or not self.current_step:
            return
            
        now = time.time()
        if now - self.last_update < 0.1:
            return
            
        # Calculate progress
        elapsed_ms = (now - self.current_step.start_time) * 1000
        progress = self.progress.update(elapsed_ms)
        
        # Move cursor to start of line and clear it
        print('\r\033[2K', end='')
        
        # Build status line
        context = f" [{self.current_step.context}]" if self.current_step.context else ""
        status = f"{ORANGE}[{len(self.steps) + 1:02d}] [{os.path.basename(self.current_step.file)}] [{self.current_step.function}]{context} [{spinner_char}] {self.current_step.description} {progress}"
        
        # Add counts
        counts = []
        if self.current_step.systems_remaining is not None:
            systems_total = max(1, self.table_totals['systems'])  # Avoid division by zero
            percent = (self.current_step.systems_remaining / systems_total * 100)
            counts.append(f"Systems: {self.current_step.systems_remaining:,} ({percent:.1f}%)")
            
        if self.current_step.stations_remaining is not None:
            stations_total = max(1, self.table_totals['stations'])
            percent = (self.current_step.stations_remaining / stations_total * 100)
            counts.append(f"Stations: {self.current_step.stations_remaining:,} ({percent:.1f}%)")
            
        if self.current_step.signals_remaining is not None:
            signals_total = max(1, self.table_totals['mineral_signals'])
            percent = (self.current_step.signals_remaining / signals_total * 100)
            counts.append(f"Signals: {self.current_step.signals_remaining:,} ({percent:.1f}%)")
            
        if self.current_step.commodities_remaining is not None:
            commodities_total = max(1, self.table_totals['station_commodities'])
            percent = (self.current_step.commodities_remaining / commodities_total * 100)
            counts.append(f"Commodities: {self.current_step.commodities_remaining:,} ({percent:.1f}%)")
            
        if counts:
            status += f" | {' | '.join(counts)}"
            
        # Print status line and stay on same line
        print(status + RESET, end='', flush=True)
        self.last_update = now
        
    def _write_step_log(self):
        """Write current step to log file"""
        if not self.current_step or IS_PRODUCTION:
            return
            
        with open(self.log_file, 'a', encoding='utf-8') as f:
            step = self.current_step
            
            # Write step header
            f.write(f"\n{'-'*80}\n")
            f.write(f"Step {step.step_number}: {step.description}\n")
            f.write(f"File: {os.path.basename(step.file)}, Function: {step.function}\n")
            f.write(f"Duration: {step.duration_ms:.1f}ms\n")
            
            if step.context:
                f.write(f"Context: {step.context}\n")
                
            if step.query_text:
                f.write(f"\nQuery:\n{step.query_text}\n")
            
            # Write table access details
            for table_name, metrics in step.tables.items():
                f.write(f"\nTable: {table_name}\n")
                f.write(f"  Total rows: {metrics.total_rows:,}\n")
                f.write(f"  Rows read: {metrics.rows_read:,} ({metrics.rows_read/metrics.total_rows*100:.1f}%)\n")
                f.write(f"  Columns read: {', '.join(sorted(metrics.columns_read))}\n")
                f.write(f"  Total cells read: {metrics.cells_read:,}\n")
                f.write(f"  Rows after filtering: {metrics.last_filtered_count:,}\n")
                if metrics.operations:
                    f.write(f"  Operations:\n    " + "\n    ".join(metrics.operations) + "\n")
                
            # Write remaining counts
            f.write("\nRemaining after filtering:\n")
            if step.systems_remaining:
                f.write(f"  Systems: {step.systems_remaining:,}\n")
            if step.stations_remaining:
                f.write(f"  Stations: {step.stations_remaining:,}\n")
            if step.signals_remaining:
                f.write(f"  Signals: {step.signals_remaining:,}\n")
            if step.commodities_remaining:
                f.write(f"  Commodities: {step.commodities_remaining:,}\n")
                
            f.write(f"{'-'*80}\n")
            
    def show_summary(self):
        """Show final performance summary"""
        if not self.steps or IS_PRODUCTION:
            return
            
        total_duration = sum(step.duration_ms for step in self.steps)
        
        summary = [
            f"\n{ORANGE}FINAL PERFORMANCE SUMMARY",
            "=" * 40,
            f"Total Steps: {len(self.steps)}",
            f"Total Duration: {total_duration:.1f}ms",
            "\nStep Progression:",
        ]
        
        # Show progression through steps
        for step in self.steps:
            summary.append(f"\nStep {step.step_number:02d}: {step.description}")
            summary.append(f"  Duration: {step.duration_ms:.1f}ms")
            
            # Add counts for each step
            counts = []
            if step.systems_remaining:
                counts.append(f"Systems: {step.systems_remaining:,}")
            if step.stations_remaining:
                counts.append(f"Stations: {step.stations_remaining:,}")
            if step.signals_remaining:
                counts.append(f"Signals: {step.signals_remaining:,}")
            if step.commodities_remaining:
                counts.append(f"Commodities: {step.commodities_remaining:,}")
            if counts:
                summary.append("  " + " | ".join(counts))
            
        # Print summary to console
        print("\n".join(summary) + RESET)
        
        # Write summary to log
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write("\nFINAL PERFORMANCE SUMMARY\n")
            f.write("=" * 40 + "\n")
            f.write(f"Total Steps: {len(self.steps)}\n")
            f.write(f"Total Duration: {total_duration:.1f}ms\n\n")
            f.write("Step Progression:\n")
            for step in self.steps:
                f.write(f"\nStep {step.step_number:02d}: {step.description}\n")
                f.write(f"  Duration: {step.duration_ms:.1f}ms\n")
                if step.systems_remaining:
                    f.write(f"  Systems: {step.systems_remaining:,}\n")
                if step.stations_remaining:
                    f.write(f"  Stations: {step.stations_remaining:,}\n")
                if step.signals_remaining:
                    f.write(f"  Signals: {step.signals_remaining:,}\n")
                if step.commodities_remaining:
                    f.write(f"  Commodities: {step.commodities_remaining:,}\n")

    def wrap_connection(self, connection):
        """Wraps a database connection to track all cursors"""
        if not PERF_TRACKING:
            return connection

        # Reset tracker state for new connection/search
        self.reset()

        # Initialize table counts on first connection
        self._init_table_counts(connection)

        class TrackedConnection:
            def __init__(self, connection, tracker):
                self._connection = connection
                self._tracker = tracker
                
            def cursor(self, *args, **kwargs):
                cursor = self._connection.cursor(*args, **kwargs)
                return self._tracker.wrap_cursor(cursor)
                
            def __getattr__(self, attr):
                return getattr(self._connection, attr)
                
        return TrackedConnection(connection, self)

    def wrap_cursor(self, cursor):
        """Wraps a cursor to track query execution"""
        if not PERF_TRACKING:
            return cursor

        # Define step patterns at class level
        step_patterns = [
            ("RELEVANT_SYSTEMS", "MINEABLE_SYSTEMS", "Step 1: Filter systems by distance"),
            ("FILTERED_BY_POWER", "CONTROL_SYSTEMS", "Step 2: Filter by power conditions"),
            ("FILTERED_BY_STATE", "SYSTEM_STATE", "Step 3: Filter by system state"),
            ("MINABLE_MATERIALS", "MINING_SYSTEMS", "Step 4: Find valid mining systems"),
            ("FILTERED_STATIONS", "STATION_COMMODITIES", "Step 5: Look at stations for valid mining systems"),
            ("STATION_MATERIALS", "VALID_PAIRS", "Step 6: Match stations with minerals"),
            ("BEST_PRICES", "RANKED_STATIONS", "Step 7: Get highest price per system")
        ]

        class TrackedCursor:
            def __init__(self, cursor, tracker):
                self._cursor = cursor
                self._tracker = tracker
                self._file = self._get_caller_info()[0]
                self._function = self._get_caller_info()[1]
                self._step_patterns = step_patterns  # Store patterns in instance
                
            def _get_caller_info(self):
                """Get file and function name from call stack"""
                frame = inspect.currentframe()
                skip_funcs = ['wrap_cursor', '_get_caller_info', 'cursor', 'execute', 'get_db_connection']
                skip_files = ['perf_tracker.py', 'common.py']
                
                while frame:
                    code = frame.f_code
                    if (code.co_name not in skip_funcs and 
                        os.path.basename(code.co_filename) not in skip_files):
                        return os.path.basename(code.co_filename), code.co_name
                    frame = frame.f_back
                return "unknown", "unknown"
                
            def set_context(self, file: str, function: str):
                """Set current execution context"""
                self._file = file
                self._function = function
                return self
                
            def execute(self, query, params=None):
                """Execute query with tracking"""
                if not PERF_TRACKING:
                    return self._cursor.execute(query, params)
                
                # Get operation description
                operation = self._get_operation_description(query)
                
                # Only start a new step if:
                # 1. No current step exists, or
                # 2. This is a new distinct operation
                if not self._tracker.current_step:
                    self._tracker.start_step(operation)
                    # Set initial context from caller
                    self._tracker.update_context(self._file, self._function, "")
                elif operation != self._tracker.current_step.description:
                    self._tracker.end_step()
                    self._tracker.start_step(operation)
                    # Set initial context from caller
                    self._tracker.update_context(self._file, self._function, "")
                
                try:
                    # Get query plan
                    plan_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                    self._cursor.execute(plan_query, params)
                    plan = self._cursor.fetchone()[0][0]
                    
                    # Process plan and extract metrics
                    self._process_plan(plan)
                    
                    # Execute actual query
                    result = self._cursor.execute(query, params)
                    
                    # Update progress bar - only if this is not a context update
                    if self._tracker.spinner and self._tracker.spinner.chars:
                        self._tracker._update_display(self._tracker.spinner.chars[self._tracker.spinner.index])
                    
                    # Check if this is the final query of a search
                    is_final_query = (
                        # Main search result query
                        (query.strip().upper().startswith('SELECT') and 
                         'ORDER BY' in query.upper() and 
                         'LIMIT' in query.upper() and
                         not 'EXPLAIN' in query.upper()) or
                        # Other commodities query
                        ('station_commodities' in query.lower() and 
                         'commodity_name' in query.lower() and
                         'sell_price > 0' in query) or
                        # Final step query
                        ('best_prices' in query.lower())
                    )
                    
                    if is_final_query and self._tracker.current_step:
                        self._tracker.end_step()
                        # Only show summary at the very end
                        if 'ORDER BY' in query.upper():
                            self._tracker.show_summary()
                    
                    return result
                    
                except Exception as e:
                    print(f"{ORANGE}[PERF] Query execution failed: {str(e)}{RESET}")
                    return self._cursor.execute(query, params)
                
            def _process_plan(self, plan):
                """Process query plan to extract statistics"""
                def process_node(node):
                    # Extract table statistics
                    if "Relation Name" in node:
                        table_name = node["Relation Name"]
                        
                        # Get columns from node
                        columns = set()
                        for key in ["Target List", "Output"]:
                            if key in node:
                                for item in node[key]:
                                    if isinstance(item, dict) and "TargetName" in item:
                                        columns.add(item["TargetName"].split(".")[-1])
                                    elif isinstance(item, str):
                                        columns.add(item.split(".")[-1])
                        
                        # Get rows read and filtered - use actual rows directly
                        rows_read = node.get("Actual Rows", 0)
                        
                        # Track metrics
                        if not table_name in self._tracker.current_step.tables:
                            self._tracker.current_step.tables[table_name] = TableMetrics(
                                name=table_name,
                                total_rows=self._tracker.table_totals.get(table_name, 0)
                            )
                        
                        metrics = self._tracker.current_step.tables[table_name]
                        metrics.rows_read = rows_read  # Just use actual rows
                        metrics.columns_read.update(columns)
                        metrics.cells_read = rows_read * len(columns)
                        metrics.last_filtered_count = rows_read
                        
                        # Add operation description
                        op_desc = []
                        if "Filter" in node:
                            op_desc.append(f"Filter: {node['Filter']}")
                        if "Index Cond" in node:
                            op_desc.append(f"Index: {node['Index Cond']}")
                        if op_desc:
                            metrics.operations = op_desc  # Replace, don't append
                        
                        # Update remaining counts based on actual rows
                        if table_name == "systems":
                            self._tracker.update_remaining_counts(systems=rows_read)
                        elif table_name == "stations":
                            self._tracker.update_remaining_counts(stations=rows_read)
                        elif table_name == "mineral_signals":
                            self._tracker.update_remaining_counts(signals=rows_read)
                        elif table_name == "station_commodities" or table_name == "station_commodities_mapped":
                            self._tracker.update_remaining_counts(commodities=rows_read)
                    
                    # Process child nodes
                    for child in node.get("Plans", []):
                        process_node(child)
                
                process_node(plan["Plan"])
                
            def _get_operation_description(self, query):
                """Get human-readable description of the query operation"""
                query = query.strip().upper()
                
                # Check for specific query types
                if query.startswith('SELECT'):
                    if 'SELECT x, y, z FROM systems WHERE name' in query:
                        return "Getting reference coordinates"
                    elif 'RELEVANT_SYSTEMS' in query:
                        return "Step 1: Filter systems by distance"
                    elif 'FILTERED_BY_POWER' in query:
                        return "Step 2: Filter by power conditions"
                    elif 'FILTERED_BY_STATE' in query:
                        return "Step 3: Filter by system state"
                    elif 'MINABLE_MATERIALS' in query:
                        return "Step 4: Find valid mining systems"
                    elif 'FILTERED_STATIONS' in query:
                        return "Step 5: Look at stations for valid mining systems"
                    elif 'STATION_MATERIALS' in query:
                        return "Step 6: Match stations with minerals"
                    elif 'BEST_PRICES' in query:
                        return "Step 7: Get highest price per system"
                    elif 'mineral_signals' in query.lower() and 'systems' in query.lower():
                        return "Getting mineral signals"
                    elif 'station_commodities' in query.lower():
                        return "Getting station commodity data"
                    elif 'systems' in query.lower():
                        return "Getting system data"
                
                return "Executing query"

            def __getattr__(self, attr):
                return getattr(self._cursor, attr)
                
        return TrackedCursor(cursor, self)

# Global instance
tracker = QueryTracker()

def track_step(description: str):
    """Decorator to track function execution"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not PERF_TRACKING:
                return func(*args, **kwargs)
                
            tracker.start_step(description)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                tracker.end_step()
        return wrapper
    return decorator

def update_tracking(file: str, function: str, context: str):
    """Update tracking context - safe to call even if tracking is disabled"""
    if PERF_TRACKING:
        try:
            tracker.update_context(file, function, context)
        except Exception:
            pass  # Silently ignore any tracking errors

