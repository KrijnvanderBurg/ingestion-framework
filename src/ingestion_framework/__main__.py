"""
hello_world
================

This module contains the main entry point for the Hello World application.

note: Sphinx does not add __main__.py or __init__.py to the docs output.

Functions
---------
main()
    Prints filepath argument to the console.
"""

from argparse import ArgumentParser, Namespace


def main(filepath: str) -> None:
    """
    Main entry point for the Hello World application.

    Prints 'Hello, World!' to the console.

    Args:
        filepath (str): The path to a file.
    """

    if filepath != "":
        print(filepath)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--filepath", required=True, type=str)
    args: Namespace = parser.parse_args()

    main(filepath=args.filepath)
