import pandas as pd


def preprocess_bos_module_cost(df, module_name, multiplier):
    """
    Preprocesses ALL line items for a specific module in a BOS cost dataframe
    by applying a multiplier for all types of costs. It also adds a column
    called f'{module_name} multiplier' that notes the multiplier of that module.

    It appends the new dataframe onto the old dataframe.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe with all the cost data. It will be left unmodified

    module_name: str
        The name of a module, like FoundationCost or ErectionCost

    multiplier: float
        The multiplier to place on every cost

    Returns
    -------
    pd.DataFrame
        The dataframe with the column added
    """
    module_multiplier_column_name = f'{module_name} multiplier'

    original = df.copy()
    original[module_multiplier_column_name] = 1.0

    modified_list = []
    for _, row in original.iterrows():
        new_row = row.copy()
        new_row[module_multiplier_column_name] = multiplier
        if new_row['Module'] == module_name:
            new_row['Cost per project'] *= multiplier
        modified_list.append(new_row)
    modified = pd.DataFrame(modified_list)
    result = original.append(modified, sort=False, ignore_index=True)
    return result


def main():
    # Select every row from the AEP and TCC files
    print('Reading AEP and TCC data...')
    aep = pd.read_csv('aep.csv')
    tcc = pd.read_csv('tcc.csv')

    # Select every row from the LandBOSSE output.
    # Only select the needed columns, convert MW to kW and rename the columns
    # to be consistent with the AEP and TCC data.
    print('Reading BOS data...')
    bos = pd.read_csv('extended_landbosse_costs.csv')
    bos = bos[
        ['Number of turbines', 'Turbine rating MW', 'Hub height m', 'Labor cost multiplier', 'Crane breakdown fraction',
         'Rotor diameter m', 'Cost per project', 'Module']]

    # Pre-process the BOS model cost
    print(f'Modifying BOS data for 50% foundation cost. Original row count {len(bos)}')
    bos = preprocess_bos_module_cost(bos, 'FoundationCost', 0.5)
    print(f'Done modifying BOS data. New row count {len(bos)}')

    bos['Rating [kW]'] = bos['Turbine rating MW'] * 1000
    bos.rename(columns={
        'Rotor diameter m': 'Rotor Diam [m]',
        'Cost per project': 'BOS Capex [USD]',
        'Hub height m': 'Hub height [m]'
    }, inplace=True)
    bos.drop(columns=['Turbine rating MW'], inplace=True)

    # Aggregate and sum BOS costs
    print('Aggregating BOS costs...')
    bos_sum = bos.groupby(
        ['Rating [kW]', 'Rotor Diam [m]', 'Number of turbines', 'Hub height [m]', 'Labor cost multiplier',
         'Crane breakdown fraction', 'FoundationCost multiplier']).sum().reset_index()

    # Inner join AEP and TCC. Taken together, Rating [kW] and Rotor Diam [m] and Hub height [m]
    # are the key.
    print('Joining AEP and TCC...')
    aep_tcc = aep.merge(tcc, on=['Rating [kW]', 'Rotor Diam [m]', 'Hub height [m]'])

    if len(aep_tcc) == 0:
        raise Exception('aep_tcc merge is empty')

    # Then join in the BOS sum data, again using Rating [kW] and
    # Rotor Diam [m] as keys. This dataframe will eventually have the
    # LCOE as a column.
    print('Joining aep_tcc and bos_sum...')
    lcoe = aep_tcc.merge(bos_sum, on=['Rating [kW]', 'Hub height [m]'])

    if len(bos_sum) == 0:
        raise Exception('bos_sum is empty.')

    print('Calculatig the LCOE...')
    # Create columns for FCR and Opex USD/kW
    lcoe['FCR [/yr]'] = 0.079
    lcoe['Opex [USD/kW/yr]'] = 52.0

    # Now calculate LCOE and save the intermediate columns
    lcoe['Total Opex [USD]'] = lcoe['Opex [USD/kW/yr]'] * lcoe['Rating [kW]'] * lcoe['Number of turbines']
    lcoe['Turbine Capex [USD]'] = lcoe['TCC [USD/kW]'] * lcoe['Rating [kW]'] * lcoe['Number of turbines']
    capex_times_fcr = (lcoe['BOS Capex [USD]'] + lcoe['Turbine Capex [USD]']) * lcoe['FCR [/yr]']
    aep_all_turbines = lcoe['AEP [kWh/yr]'] * lcoe['Number of turbines']
    lcoe['LCOE [USD/kWh]'] = (capex_times_fcr + lcoe['Total Opex [USD]']) / aep_all_turbines

    print(f'Writing LCOE analysis with {len(lcoe)} rows...')
    lcoe.to_csv('lcoe_analysis.csv', index=False)

    print('Done writing LCOE analysis.')


if __name__ == '__main__':
    main()
