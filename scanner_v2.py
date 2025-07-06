#!/usr/bin/env python3
"""
Parallel Xray Network Performance Testing Script
Tests multiple Xray proxy configurations in parallel with different ports
Now supports batching for running all combinations in parallel batches
"""

import json
import math
import platform
import random
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from pathlib import Path
from typing import Dict, List, Tuple

import requests


class ParallelXrayTester:
    def __init__(self):
        self.script_dir = Path(__file__).parent
        self.xray_path = (
            self.script_dir / "xray.exe"
            if platform.system() == "Windows"
            else self.script_dir / "xray"
        )
        self.base_config_path = self.script_dir / "config.json"
        self.log_file = self.script_dir / "pings.txt"
        self.base_log_file = self.script_dir / "xraylogs.txt"
        self.results_file = self.script_dir / "results.txt"

        # Configuration options
        self.packets_options = ["1-1", "1-2", "1-3"]
        self.length_options = [str(i) for i in range(1, 51)]
        self.interval_options = [str(i) for i in range(1, 51)]

        # Calculate maximum combinations
        self.max_combinations = (
            len(self.packets_options)
            * len(self.length_options)
            * len(self.interval_options)
        )

        self.xray_processes = {}
        self.config_files = {}
        self.log_files = {}
        self.lock = threading.Lock()

    def check_os_compatibility(self) -> bool:
        """Check if running on supported OS"""
        os_name = platform.system()
        if os_name not in ["Windows", "Linux", "Darwin"]:
            print(f"Unsupported operating system: {os_name}")
            return False
        return True

    def setup_files(self):
        """Create log files if they don't exist and clear their content"""
        # Check if xray executable exists
        if not self.xray_path.exists():
            print(f"Error: {self.xray_path.name} not found")
            sys.exit(1)

        # Check if base config exists
        if not self.base_config_path.exists():
            print(f"Error: {self.base_config_path.name} not found")
            sys.exit(1)

        # Create and clear main log file
        self.log_file.touch()
        self.log_file.write_text("")

        # Create and clear results file
        self.results_file.touch()
        self.results_file.write_text("")

    def get_user_input(self) -> Dict[str, int]:
        """Get user input with default values"""
        try:
            print(f"Maximum possible combinations: {self.max_combinations}")

            instances_input = input(
                f"Enter the number of instances to test (default is {self.max_combinations} - all combinations): "
            ).strip()
            instances = (
                int(instances_input) if instances_input else self.max_combinations
            )

            parallel_input = input(
                "Enter the number of parallel instances to run at once (default is 5): "
            ).strip()
            parallel_instances = int(parallel_input) if parallel_input else 5

            timeout_input = input(
                "Enter the timeout for each ping test in seconds (default is 10): "
            ).strip()
            timeout_sec = int(timeout_input) if timeout_input else 10

            ping_count_input = input(
                "Enter the number of requests per instance (default is 3): "
            ).strip()
            ping_count = (int(ping_count_input) if ping_count_input else 3) + 1

            return {
                "instances": instances,
                "parallel_instances": parallel_instances,
                "timeout_sec": timeout_sec,
                "ping_count": ping_count,
            }
        except ValueError:
            print("Invalid input. Please enter numeric values.")
            return self.get_user_input()

    def validate_instances(self, instances: int) -> int:
        """Validate and adjust instances count"""
        if instances > self.max_combinations:
            print(
                f"Warning: Maximum possible unique combinations is {self.max_combinations}. "
                f"Setting instances to {self.max_combinations}."
            )
            instances = self.max_combinations

        return instances

    def get_all_combinations(self, instances: int) -> List[Tuple[str, str, str]]:
        """Get all possible combinations or random unique combinations"""
        all_combinations = list(
            product(self.packets_options, self.length_options, self.interval_options)
        )

        if instances <= len(all_combinations):
            return random.sample(all_combinations, instances)
        else:
            # If we need more instances than combinations, repeat combinations
            combinations = []
            for i in range(instances):
                combinations.append(all_combinations[i % len(all_combinations)])
            return combinations

    def create_config_file(
        self, instance_id: int, packets: str, length: str, interval: str, port: int
    ) -> str:
        """Create a unique config file for each instance"""
        try:
            with open(self.base_config_path, "r") as f:
                config = json.load(f)

            # Update the HTTP inbound port
            for inbound in config.get("inbounds", []):
                if inbound.get("tag") == "http":
                    inbound["port"] = port
                    break

            # Find the outbound with tag 'fragment' and modify its fragment settings
            fragment_outbound = None
            for outbound in config.get("outbounds", []):
                if outbound.get("tag") == "fragment":
                    fragment_outbound = outbound
                    break

            if fragment_outbound:
                if "settings" not in fragment_outbound:
                    fragment_outbound["settings"] = {}
                if "fragment" not in fragment_outbound["settings"]:
                    fragment_outbound["settings"]["fragment"] = {}

                fragment_outbound["settings"]["fragment"]["packets"] = packets
                fragment_outbound["settings"]["fragment"]["length"] = length
                fragment_outbound["settings"]["fragment"]["interval"] = interval
            else:
                print(
                    f"Warning: No 'fragment' outbound found in config for instance {instance_id}"
                )

            # Save config to unique file
            config_file = self.script_dir / f"config_{instance_id}.json"
            with open(config_file, "w") as f:
                json.dump(config, f, indent=2)

            return str(config_file)

        except Exception as e:
            print(f"Error creating config for instance {instance_id}: {e}")
            raise Exception("Error creating config for instance")

    def start_xray_instance(self, instance_id: int, config_file: str) -> bool:
        """Start a single Xray instance"""
        try:
            log_file = self.script_dir / f"xraylogs_{instance_id}.txt"
            self.log_files[instance_id] = log_file

            with open(log_file, "w") as f:
                process = subprocess.Popen(
                    [str(self.xray_path), "-c", config_file],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=str(self.script_dir),
                )

            self.xray_processes[instance_id] = process
            time.sleep(2)  # Wait for process to start

            # Check if process is still running
            if process.poll() is None:
                return True
            else:
                print(f"Failed to start Xray instance {instance_id}")
                return False

        except Exception as e:
            print(f"Error starting Xray instance {instance_id}: {e}")
            return False

    def stop_xray_instance(self, instance_id: int):
        """Stop a single Xray instance"""
        try:
            if instance_id in self.xray_processes:
                process = self.xray_processes[instance_id]
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                del self.xray_processes[instance_id]
        except Exception as e:
            print(f"Error stopping Xray instance {instance_id}: {e}")

    def stop_all_xray_processes(self):
        """Stop all Xray processes"""
        for instance_id in list(self.xray_processes.keys()):
            self.stop_xray_instance(instance_id)

        # Also try to kill any remaining xray processes
        try:
            if platform.system() == "Windows":
                subprocess.run(
                    ["taskkill", "/F", "/IM", "xray.exe"],
                    capture_output=True,
                    check=False,
                )
            else:
                subprocess.run(
                    ["pkill", "-f", "xray"], capture_output=True, check=False
                )
        except Exception:
            pass

    def send_http_request(
        self, instance_id: int, port: int, ping_count: int, timeout_sec: int
    ) -> Tuple[float, List[float]]:
        """Perform HTTP requests with proxy and measure response time"""
        url = "http://cp.cloudflare.com"
        proxy_server = "127.0.0.1"

        proxies = {
            "http": f"http://{proxy_server}:{port}",
            "https": f"http://{proxy_server}:{port}",
        }

        individual_times = []

        # Ping the specified number of times and measure the time for each ping
        for i in range(ping_count):
            try:
                start_time = time.time()
                response = requests.get(url, proxies=proxies, timeout=timeout_sec)
                end_time = time.time()

                elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
                individual_times.append(elapsed_time)

            except requests.exceptions.RequestException:
                individual_times.append(-1)  # Mark failed requests with -1

            # Add a small delay between each ping
            time.sleep(0.5)

        # Skip the first ping result
        individual_times = individual_times[1:]

        # Calculate average ping time, considering -1 as timeout
        valid_pings = [t for t in individual_times if t != -1]
        if valid_pings:
            total_valid_time = sum(valid_pings)
            timeout_penalty = (len(individual_times) - len(valid_pings)) * (
                timeout_sec * 1000
            )
            average_ping = (total_valid_time + timeout_penalty) / len(individual_times)
        else:
            average_ping = 0

        # Log individual ping times
        with self.lock:
            with open(self.log_file, "a") as f:
                f.write(
                    f"Instance {instance_id} (Port {port}) - Individual Ping Times:\n"
                )
                f.write(",".join(map(str, individual_times)) + "\n")

        return average_ping, individual_times

    def test_instance(
        self,
        instance_id: int,
        packets: str,
        length: str,
        interval: str,
        port: int,
        ping_count: int,
        timeout_sec: int,
    ) -> Dict | None:
        """Test a single instance configuration"""
        try:
            # Create config file
            config_file = self.create_config_file(
                instance_id, packets, length, interval, port
            )
            self.config_files[instance_id] = config_file

            # Start Xray instance
            if not self.start_xray_instance(instance_id, config_file):
                return None

            # Wait a bit for the proxy to be ready
            time.sleep(3)

            # Log test parameters
            with self.lock:
                with open(self.log_file, "a") as f:
                    f.write(
                        f"Instance {instance_id}: Testing packets={packets}, length={length}, interval={interval}, port={port}\n"
                    )

            # Perform HTTP requests
            average_ping, individual_pings = self.send_http_request(
                instance_id, port, ping_count, timeout_sec
            )

            # Log results
            with self.lock:
                with open(self.log_file, "a") as f:
                    f.write(
                        f"Instance {instance_id}: Average Ping Time: {average_ping:.2f} ms\n\n"
                    )

            return {
                "Instance": instance_id,
                "Packets": packets,
                "Length": length,
                "Interval": interval,
                "Port": port,
                "AverageResponseTime": average_ping,
                "IndividualPings": individual_pings,
            }

        except Exception as e:
            print(f"Error testing instance {instance_id}: {e}")
            return None
        finally:
            # Clean up
            self.stop_xray_instance(instance_id)
            # Clean up config file
            if instance_id in self.config_files:
                try:
                    Path(self.config_files[instance_id]).unlink(missing_ok=True)
                except:
                    pass

    def print_table_header(self):
        """Print the table header"""
        header = """+----------+---------+----------+----------+------+---------------+
| Instance | Packets |  Length  | Interval | Port | Average Ping  |
+----------+---------+----------+----------+------+---------------+"""
        print(header)

    def print_table_row(
        self,
        instance: int,
        packets: str,
        length: str,
        interval: str,
        port: int,
        avg_ping: float,
    ):
        """Print a table row"""
        avg_ping_str = f"{avg_ping:.2f}" if avg_ping > 0 else "Failed"
        row = f"| {instance:^8} | {packets:^7} | {length:^8} | {interval:^8} | {port:^4} | {avg_ping_str:^13} |"
        print(row)

    def print_table_footer(self):
        """Print the table footer"""
        footer = "+----------+---------+----------+----------+------+---------------+"
        print(footer)

    def write_results_to_file(self, results: List[Dict]):
        """Write results to results.txt file"""
        try:
            with open(self.results_file, "w") as f:
                f.write("Xray Network Performance Test Results\n")
                f.write("=" * 50 + "\n\n")

                f.write(
                    f"{'Instance':<9} {'Packets':<8} {'Length':<8} {'Interval':<9} {'Port':<5} {'Avg Time (ms)':<15} Individual Pings (ms)\n"
                )
                f.write("-" * 120 + "\n")

                for result in results:
                    individual_pings_str = ", ".join(
                        [
                            f"{p:.2f}" if p != -1 else "FAIL"
                            for p in result["IndividualPings"]
                        ]
                    )
                    f.write(
                        f"{result['Instance']:<9} {result['Packets']:<8} {result['Length']:<8} "
                        f"{result['Interval']:<9} {result['Port']:<5} {result['AverageResponseTime']:<15.2f} [{individual_pings_str}]\n"
                    )
        except Exception as e:
            print(f"Error writing results to file: {e}")

    def print_sorted_results(self, results: List[Dict]):
        """Print sorted results to terminal"""
        valid_results = [r for r in results if r["AverageResponseTime"] > 0]
        sorted_results = sorted(valid_results, key=lambda x: x["AverageResponseTime"])

        print("\nAll successful results (sorted by average ping time):")
        if sorted_results:
            print(
                f"{'Instance':<9} {'Packets':<8} {'Length':<8} {'Interval':<9} {'Port':<5} {'Avg Time (ms)':<15} Individual Pings (ms)"
            )
            print("-" * 120)
            for result in sorted_results:
                individual_pings_str = ", ".join(
                    [
                        f"{p:.2f}" if p != -1 else "FAIL"
                        for p in result["IndividualPings"]
                    ]
                )
                print(
                    f"{result['Instance']:<9} {result['Packets']:<8} {result['Length']:<8} "
                    f"{result['Interval']:<9} {result['Port']:<5} {result['AverageResponseTime']:<15.2f} [{individual_pings_str}]"
                )
        else:
            print("No valid results found.")

        return sorted_results
        """Clean up temporary files"""
        try:
            # Clean up config files
            for config_file in self.config_files.values():
                try:
                    Path(config_file).unlink(missing_ok=True)
                except:
                    pass

            # Clean up log files
            for log_file in self.log_files.values():
                try:
                    log_file.unlink(missing_ok=True)
                except:
                    pass
        except:
            pass

    def run_batch(
        self,
        batch_combinations: List[Tuple],
        batch_num: int,
        total_batches: int,
        config: Dict,
    ) -> List[Dict]:
        """Run a batch of tests"""
        print(
            f"\nRunning batch {batch_num}/{total_batches} ({len(batch_combinations)} tests)..."
        )

        batch_results = []

        try:
            with ThreadPoolExecutor(max_workers=len(batch_combinations)) as executor:
                # Submit all tasks for this batch
                future_to_instance = {}
                for i, (instance_id, packets, length, interval, port) in enumerate(
                    batch_combinations
                ):
                    future = executor.submit(
                        self.test_instance,
                        instance_id,
                        packets,
                        length,
                        interval,
                        port,
                        config["ping_count"],
                        config["timeout_sec"],
                    )
                    future_to_instance[future] = (
                        instance_id,
                        packets,
                        length,
                        interval,
                        port,
                    )

                # Process completed tasks
                for future in as_completed(future_to_instance):
                    instance_id, packets, length, interval, port = future_to_instance[
                        future
                    ]
                    try:
                        result = future.result()
                        if result:
                            batch_results.append(result)
                            # Print table row
                            self.print_table_row(
                                result["Instance"],
                                result["Packets"],
                                result["Length"],
                                result["Interval"],
                                result["Port"],
                                result["AverageResponseTime"],
                            )
                        else:
                            # Print failed test
                            self.print_table_row(
                                instance_id, packets, length, interval, port, 0
                            )
                    except Exception as e:
                        print(f"Instance {instance_id} generated an exception: {e}")
                        self.print_table_row(
                            instance_id, packets, length, interval, port, 0
                        )

        except Exception as e:
            print(f"Error in batch {batch_num}: {e}")

        return batch_results

    def run_tests(self):
        """Main test execution function"""
        if not self.check_os_compatibility():
            return

        self.setup_files()

        # Get user input
        config = self.get_user_input()
        config["instances"] = self.validate_instances(config["instances"])

        # Get all combinations to test
        combinations = self.get_all_combinations(config["instances"])

        # Calculate number of batches
        total_batches = math.ceil(len(combinations) / config["parallel_instances"])

        print(f"\nRunning {len(combinations)} tests in {total_batches} batches...")
        print(
            f"Each batch will run up to {config['parallel_instances']} tests in parallel"
        )
        print("This may take a while as each test runs independently...\n")

        # Store results
        results = []
        completed_tests = 0

        # Add current_results attribute for signal handler
        self.current_results = results

        # Print table header
        self.print_table_header()

        try:
            # Process combinations in batches
            for batch_num in range(total_batches):
                start_idx = batch_num * config["parallel_instances"]
                end_idx = min(
                    start_idx + config["parallel_instances"], len(combinations)
                )

                # Prepare batch combinations with instance IDs and ports (reuse ports)
                batch_combinations = []
                for i in range(start_idx, end_idx):
                    packets, length, interval = combinations[i]
                    instance_id = i + 1
                    port = 2000 + (i % config["parallel_instances"])  # Reuse ports
                    batch_combinations.append(
                        (instance_id, packets, length, interval, port)
                    )

                # Run the batch
                batch_results = self.run_batch(
                    batch_combinations, batch_num + 1, total_batches, config
                )
                results.extend(batch_results)

                completed_tests += len(batch_combinations)
                print(
                    f"Batch {batch_num + 1} completed. Progress: {completed_tests}/{len(combinations)} tests completed"
                )

                # Small delay between batches to avoid overwhelming the system
                if batch_num < total_batches - 1:
                    time.sleep(2)

        except KeyboardInterrupt:
            print("\nTest interrupted by user. Cleaning up...")
            # Print and save results before cleanup
            if results:
                print("\nGenerating final results...")
                sorted_results = self.print_sorted_results(results)
                self.write_results_to_file(sorted_results)
                print(f"\nResults saved to {self.results_file}")
        finally:
            # Always stop all processes and clean up
            self.stop_all_xray_processes()
            self.cleanup_files()

        # Print table footer
        self.print_table_footer()

        # Generate final results
        print("\nGenerating final results...")
        sorted_results = self.print_sorted_results(results)
        self.write_results_to_file(sorted_results)
        print(f"\nResults saved to {self.results_file}")

        # Display top 10 results
        print("\nTop 10 best results:")
        if sorted_results:
            top_10 = sorted_results[:10]
            print(
                f"{'Instance':<9} {'Packets':<8} {'Length':<8} {'Interval':<9} {'Port':<5} {'Avg Time (ms)':<15}"
            )
            print("-" * 70)
            for result in top_10:
                print(
                    f"{result['Instance']:<9} {result['Packets']:<8} {result['Length']:<8} "
                    f"{result['Interval']:<9} {result['Port']:<5} {result['AverageResponseTime']:.2f}"
                )
        else:
            print("No valid results found.")

        input("\nPress Enter to exit the script...")

    def cleanup_files(self):
        """Clean up temporary files"""
        try:
            # Clean up config files
            for config_file in self.config_files.values():
                try:
                    Path(config_file).unlink(missing_ok=True)
                except:
                    pass

            # Clean up log files
            for log_file in self.log_files.values():
                try:
                    log_file.unlink(missing_ok=True)
                except:
                    pass
        except:
            pass


# Global variable to store tester instance for signal handling
_tester_instance = None


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\nReceived interrupt signal. Cleaning up...")
    global _tester_instance
    if _tester_instance:
        _tester_instance.stop_all_xray_processes()
        _tester_instance.cleanup_files()
        # If there are any results, print and save them
        if (
            hasattr(_tester_instance, "current_results")
            and _tester_instance.current_results
        ):
            print("\nGenerating final results before exit...")
            sorted_results = _tester_instance.print_sorted_results(
                _tester_instance.current_results
            )
            _tester_instance.write_results_to_file(sorted_results)
            print(f"Results saved to {_tester_instance.results_file}")
    sys.exit(0)


def main():
    """Main function"""
    global _tester_instance
    _tester_instance = ParallelXrayTester()

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    try:
        _tester_instance.run_tests()
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        _tester_instance.stop_all_xray_processes()
        _tester_instance.cleanup_files()
    except Exception as e:
        print(f"An error occurred: {e}")
        _tester_instance.stop_all_xray_processes()
        _tester_instance.cleanup_files()


if __name__ == "__main__":
    main()
