from jinja2 import Environment, FileSystemLoader, select_autoescape, StrictUndefined
import os
import json


class ConfigWriter:

    def __init__(self, template_path):
        self.env = Environment(
            loader=FileSystemLoader(template_path),
            autoescape=select_autoescape(),
            undefined=StrictUndefined
        )

    def render(self, tasks_dict, template_name, output_file_path, check_json=False):
        output_path = output_file_path.format(tasks_dict.get('default'))
        self.delete_files(output_path)
        template = self.env.get_template(template_name)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(template.render(tasks_dict))
        if str(output_path).endswith(".json") and check_json:
            with open(f'{output_path}') as json_file_value:
                config_file_dict = json.load(json_file_value)
            file_content = json.dumps(config_file_dict, indent=4)
            with open(f'{output_path}', "w") as final_config_file:
                final_config_file.write(file_content)

    @staticmethod
    def delete_files(path):
        if os.path.exists(path):
            print(f"Deleting: {path}")
            os.remove(path)
