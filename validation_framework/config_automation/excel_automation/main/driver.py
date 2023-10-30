from config_reader import ConfigReader
from config_writer import ConfigWriter
from config_generator import ConfigGenerator
import itertools


class Driver:

    def __init__(self,
                 passed_tenant="",
                 passed_group="",
                 passed_group_name="",
                 passed_excel_type="",
                 config_type="example",
                 sheet_name="Sheet1"):
        self.cfg = ConfigReader(config_type)
        self.tenant = passed_tenant
        self.group = passed_group
        self.group_name = passed_group_name
        self.excel_type = passed_excel_type
        excel_path = self.cfg.config.get('default', self.excel_type)
        print(f"passed_excel_type --> {passed_excel_type}, "
              f"excel_path --> {excel_path}")
        self.tbl_info = ConfigGenerator(self.cfg.config, excel_path, sheet_name)
        self.edm_name = ""
        template_base_path = self.cfg.config.get('default', 'template_base_path')
        self.writer = ConfigWriter(template_base_path)

    def crt_dag_group(self):
        return self.generate_configs_files("dag_template.txt", 'dag_file')

    def crt_dag_cluster_configs(self):
        return self.generate_configs_files("dag_cluster_config_template.txt",
                                           'dag_cluster_config_file')

    def crt_dag_job_configs(self, check_json):
        return self.generate_configs_files("dag_job_config_template.txt",
                                           'dag_job_config_file', check_json)

    def crt_configs(self):
        return self.generate_configs_files("keys_template.txt",
                                           'validation_config_file')

    def crt_execution_stat(self):
        return self.generate_configs_files("execution_template.txt",
                                           'execution_file')

    def crt_execution_stat_wo_nohup(self):
        return self.generate_configs_files("execution_template_wo_nohup.txt",
                                           'execution_file')

    def generate_configs_files(self, template_name, output_path, check_json=False):
        tasks = self.tbl_info.crt_task(
            group=self.group,
            group_name=self.group_name,
            passed_excel_type=self.excel_type
        )
        tasks["default"] = self.cfg.config["default"]
        output_file_path = self.cfg.config.get('default', output_path)
        self.writer.render(tasks, template_name, output_file_path, check_json)
        return output_file_path


if __name__ == '__main__':
    now_excel_type = 'validation_excel_path'
    tenants = ['example']
    group_names = ['group']
    now_config_type = "example"
    # now_config_type = "snowflake"

    # groups = [0]
    # groups = [1, 2, 3, 4, 5, 6, 7]
    # groups = [8, 9]
    # groups = [10, 11]
    # groups = [11, 12]
    groups = [12,13,14]

    comb1 = list(itertools.product(tenants, groups, group_names))
    for _tenant, _group, _group_name in comb1:
        print(f"Inside comb1 for loop, "
              f"tenant --> {_tenant}, "
              f"_group --> {_group}, "
              f"_group_name --> {_group_name}")
        obj = Driver(_tenant,
                     str(_group),
                     _group_name,
                     now_excel_type,
                     now_config_type)

        # -------- Common Framework job config generation ------------
        # obj.crt_configs()
        # # -------- Common Framework execution command generation ------------
        # obj.crt_execution_stat()
        # obj.crt_execution_stat_wo_nohup()
        # # ------------ dag generation -----------------
        # obj.crt_dag_group()
        obj.crt_dag_cluster_configs()
        obj.crt_dag_job_configs(check_json=True)
