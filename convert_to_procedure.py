# from nbconvert, convert process_data.ipynb to process_data.py

from nbconvert.nbconvertapp import NbConvertApp
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
    convert_to_procedure('process_data.ipynb', 'process_data')
