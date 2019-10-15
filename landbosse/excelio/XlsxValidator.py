import pandas as pd

class XlsxValidator:
    """
    XlsxValidator is for comparing the results of a previous model run
    to the results of a current model run.
    """

    @classmethod
    def compare_expected_to_actual(cls, expected_xlsx, actual_module_type_operation_list):
        """
        This compares the expected costs as calculated by a prior model run
        with the actual results from a current model run.

        It compares the results row by row and prints any differences.

        Parameters
        ----------
        expected_xlsx : str
            The absolute filename of the expected output .xlsx file.

        actual_module_type_operation_list : str
            The module_type_operation_list as returned by a subclass of
            XlsxManagerRunner.

        Returns
        -------
        bool
            True if the expected and actual results are equal. It returns
            False otherwise.
        """
        # First, make the list of dictionaries into a dataframe, and drop
        # the raw_cost and raw_cost_total_or_per_turbine columns.
        actual_df = pd.DataFrame(actual_module_type_operation_list)
        actual_df.drop(['raw_cost', 'raw_cost_total_or_per_turbine'], axis=1)
        expected_df = pd.read_excel(expected_xlsx, 'costs_by_module_type_operation')
        expected_df.rename(columns={
            'Project ID': 'project_id',
            'Number of turbines': 'num_turbines',
            'Turbine rating MW': 'turbine_rating_MW',
            'Module': 'module',
            'Operation ID': 'operation_id',
            'Type of cost': 'type_of_cost',
            'Cost per turbine': 'cost_per_turbine',
            'Cost per project': 'cost_per_project',
            'USD/kW per project': 'usd_per_kw_per_project'
        }, inplace=True)
        print(expected_df)
