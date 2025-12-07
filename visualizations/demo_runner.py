"""
Hackathon Demo Runner

One-click script to generate all visualizations for demo:
1. System architecture diagrams
2. Live data dashboard
3. Late data detection metrics
4. AF hierarchy visualization

Perfect for hackathon presentations - no need to access Databricks workspace!
"""

import os
import sys
import subprocess
import time
from pathlib import Path


class HackathonDemoRunner:
    """
    Orchestrate complete demo with all visualizations
    """

    def __init__(self):
        self.output_dir = Path("visualizations/output")
        self.colors = {
            'cyan': '\033[96m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'red': '\033[91m',
            'bold': '\033[1m',
            'end': '\033[0m'
        }

    def print_header(self, text):
        """Print styled header"""
        print(f"\n{self.colors['cyan']}{self.colors['bold']}{'='*100}{self.colors['end']}")
        print(f"{self.colors['cyan']}{self.colors['bold']}{text.center(100)}{self.colors['end']}")
        print(f"{self.colors['cyan']}{self.colors['bold']}{'='*100}{self.colors['end']}\n")

    def print_step(self, step_num, total, description):
        """Print step progress"""
        print(f"{self.colors['green']}[{step_num}/{total}] {description}...{self.colors['end']}")

    def print_success(self, message):
        """Print success message"""
        print(f"{self.colors['green']}✓ {message}{self.colors['end']}")

    def print_error(self, message):
        """Print error message"""
        print(f"{self.colors['red']}✗ {message}{self.colors['end']}")

    def print_warning(self, message):
        """Print warning message"""
        print(f"{self.colors['yellow']}⚠ {message}{self.colors['end']}")

    def setup_output_directory(self):
        """Create output directory for visualizations"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.print_success(f"Output directory ready: {self.output_dir}")

    def check_mock_server(self):
        """Check if mock PI server is running"""
        import requests
        try:
            response = requests.get("http://localhost:5050/piwebapi", timeout=2)
            if response.status_code == 200:
                self.print_success("Mock PI server is running")
                return True
        except:
            pass

        self.print_warning("Mock PI server not running")
        print(f"  To start: {self.colors['yellow']}python tests/mock_pi_server.py{self.colors['end']}")
        return False

    def generate_architecture_diagrams(self):
        """Generate system architecture diagrams"""
        self.print_step(1, 4, "Generating architecture diagrams")

        try:
            # Generate both diagrams
            from architecture_diagram import ArchitectureDiagramGenerator
            generator = ArchitectureDiagramGenerator()

            # Main architecture
            generator.create_architecture_diagram(
                save_path=str(self.output_dir / 'architecture_diagram.png'),
                show=False
            )
            self.print_success("  ✓ Architecture diagram created")

            # Data flow
            generator.create_data_flow_diagram(
                save_path=str(self.output_dir / 'data_flow_diagram.png'),
                show=False
            )
            self.print_success("  ✓ Data flow diagram created")

            return True
        except Exception as e:
            self.print_error(f"  Failed to generate architecture diagrams: {e}")
            return False

    def generate_late_data_viz(self):
        """Generate late data visualization"""
        self.print_step(2, 4, "Generating late data detection visualization")

        try:
            from late_data_viz import LateDataVisualizer
            viz = LateDataVisualizer()
            viz.create_visualization(
                save_path=str(self.output_dir / 'late_data_detection.png'),
                show=False
            )
            self.print_success("  ✓ Late data visualization created")
            return True
        except Exception as e:
            self.print_error(f"  Failed to generate late data viz: {e}")
            return False

    def generate_live_dashboard_image(self):
        """Generate snapshot of live dashboard"""
        self.print_step(3, 4, "Generating live dashboard snapshot")

        server_running = self.check_mock_server()

        if not server_running:
            self.print_warning("  Skipping live dashboard (server not running)")
            self.print_warning("  Start mock server first for live data visualization")
            return False

        try:
            from live_dashboard import LiveDashboard
            dashboard = LiveDashboard()
            fig, _ = dashboard.create_dashboard()

            # Save snapshot (non-animated)
            import matplotlib.pyplot as plt
            plt.savefig(str(self.output_dir / 'live_dashboard_snapshot.png'),
                       dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            self.print_success("  ✓ Live dashboard snapshot created")
            return True
        except Exception as e:
            self.print_error(f"  Failed to generate dashboard: {e}")
            return False

    def generate_metrics_report(self):
        """Generate text-based metrics report"""
        self.print_step(4, 4, "Generating metrics report")

        try:
            from late_data_viz import LateDataMetricsDashboard
            dashboard = LateDataMetricsDashboard()

            # Redirect stdout to file
            report_path = self.output_dir / 'metrics_report.txt'
            original_stdout = sys.stdout
            with open(report_path, 'w') as f:
                sys.stdout = f
                dashboard.create_metrics_display()
            sys.stdout = original_stdout

            self.print_success(f"  ✓ Metrics report created: {report_path}")
            return True
        except Exception as e:
            self.print_error(f"  Failed to generate metrics report: {e}")
            return False

    def list_generated_files(self):
        """List all generated visualization files"""
        self.print_header("Generated Files")

        files = list(self.output_dir.glob('*'))
        if not files:
            self.print_warning("No files generated")
            return

        for file in sorted(files):
            size_kb = file.stat().st_size / 1024
            print(f"  📄 {file.name:<40} ({size_kb:.1f} KB)")

    def run_interactive_mode(self):
        """Run interactive demo mode"""
        self.print_header("Interactive Demo Mode")

        print("Select visualization to view:")
        print("  1. Architecture Diagram")
        print("  2. Data Flow Diagram")
        print("  3. Late Data Detection")
        print("  4. Live Dashboard (requires mock server)")
        print("  5. Metrics Report (terminal)")
        print("  6. Generate All & Exit")
        print("  q. Quit")

        choice = input("\nEnter choice: ").strip()

        if choice == '1':
            from architecture_diagram import ArchitectureDiagramGenerator
            generator = ArchitectureDiagramGenerator()
            generator.create_architecture_diagram(show=True)

        elif choice == '2':
            from architecture_diagram import ArchitectureDiagramGenerator
            generator = ArchitectureDiagramGenerator()
            generator.create_data_flow_diagram(show=True)

        elif choice == '3':
            from late_data_viz import LateDataVisualizer
            viz = LateDataVisualizer()
            viz.create_visualization(show=True)

        elif choice == '4':
            if not self.check_mock_server():
                self.print_error("Mock server must be running for live dashboard")
                return

            from live_dashboard import LiveDashboard
            dashboard = LiveDashboard()
            fig, ani = dashboard.create_dashboard()
            import matplotlib.pyplot as plt
            plt.show()

        elif choice == '5':
            from late_data_viz import LateDataMetricsDashboard
            dashboard = LateDataMetricsDashboard()
            dashboard.create_metrics_display()

        elif choice == '6':
            self.run_full_demo()

        elif choice.lower() == 'q':
            print("Goodbye!")
            return

        else:
            self.print_error("Invalid choice")

    def run_full_demo(self):
        """Run complete demo generation"""
        self.print_header("OSIPI → Databricks Hackathon Demo Generator")

        print(f"{self.colors['bold']}This will generate all visualizations for your demo:{self.colors['end']}")
        print("  • System architecture diagrams")
        print("  • Late data detection visualizations")
        print("  • Performance metrics report")
        print("  • Live dashboard snapshot (if mock server running)")
        print()

        # Setup
        self.setup_output_directory()
        print()

        # Generate all visualizations
        results = []
        results.append(self.generate_architecture_diagrams())
        results.append(self.generate_late_data_viz())
        results.append(self.generate_live_dashboard_image())
        results.append(self.generate_metrics_report())

        # Summary
        self.print_header("Demo Generation Complete")

        success_count = sum(results)
        total_count = len(results)

        if success_count == total_count:
            self.print_success(f"All {total_count} visualizations generated successfully!")
        else:
            self.print_warning(f"{success_count}/{total_count} visualizations generated")

        print()
        self.list_generated_files()

        # Next steps
        self.print_header("Next Steps for Hackathon Demo")
        print("1. Review generated images in visualizations/output/")
        print("2. For live demo, start mock server:")
        print(f"   {self.colors['cyan']}python tests/mock_pi_server.py{self.colors['end']}")
        print("3. Then run live dashboard:")
        print(f"   {self.colors['cyan']}python visualizations/live_dashboard.py{self.colors['end']}")
        print("4. Or run interactive mode:")
        print(f"   {self.colors['cyan']}python visualizations/demo_runner.py --interactive{self.colors['end']}")
        print()
        print(f"{self.colors['green']}{self.colors['bold']}Ready to present! 🚀{self.colors['end']}")
        print()


def main():
    runner = HackathonDemoRunner()

    if '--interactive' in sys.argv or '-i' in sys.argv:
        while True:
            runner.run_interactive_mode()
            print()
            again = input("Show another visualization? (y/n): ").strip().lower()
            if again != 'y':
                break
    else:
        runner.run_full_demo()


if __name__ == "__main__":
    # Change to visualizations directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir.parent)

    main()
