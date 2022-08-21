# This script converts notebooks to python scripts

from nbconvert.nbconvertapp import NbConvertApp, os
from glob import glob
from pathlib import Path
# from nbconvert.exporters.python import PythonExporter
# from nbconvert.writers import FilesWriter


def convert_to_procedure(input_file, output_file):
    """
    Convert a notebook to a python script.
    """
    # exporter = PythonExporter()
    # writer = FilesWriter()
    app = NbConvertApp.instance()
    app.initialize(argv=[])
    app.export_format = 'python'
    app.notebooks = [input_file]
    app.output_base = output_file
    app.start()


if __name__ == '__main__':
    notebook_files = glob('./**/*.ipynb')
    print(notebook_files)
    for notebook_file in notebook_files:
        convert_to_procedure(notebook_file, Path(notebook_file).stem)
