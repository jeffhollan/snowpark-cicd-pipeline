import nbformat
notebook = nbformat.read('./most_popular_article/process_data.ipynb', as_version=4)
from nbconvert import PythonExporter
python_exporter = PythonExporter()
python_exporter.exclude_input_prompt = True
(body, resources) = python_exporter.from_notebook_node(notebook)


for cells in notebook.cells:
    if cells.cell_type == 'code':
        print(cells)