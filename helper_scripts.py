import sys
import pkg_resources


def print_installed_packages():
    """Prints the list of installed packages with their versions."""
    print("Installed packages:")
    for module_name in sys.modules:
        try:
            # Get the version using pkg_resources
            version = pkg_resources.get_distribution(module_name).version
            print(f"{module_name} version: {version}")
        except pkg_resources.DistributionNotFound:
            # If the module is not a package or version is not found, skip it
            pass