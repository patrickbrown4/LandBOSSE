import pandas as pd

if __name__ == '__main__':
    # Select every row from the AEP and TCC files
    print('Reading AEP and TCC data...')
    aep = pd.read_csv('aep.csv')
    tcc = pd.read_csv('tcc.csv')

    # Select every row from the LandBOSSE output.
    # Only select the needed columns, convert MW to kW and rename the columns
    # to be consistent with the AEP and TCC data.
    print('Reading BOS data...')
    bos = pd.read_csv('extended_landbosse_costs.csv')
    bos = bos[['Number of turbines', 'Turbine rating MW', 'Hub height m', 'Labor cost multiplier', 'Crane breakdown fraction', 'Rotor diameter m', 'Cost per project']]
    bos['Rating [kW]'] = bos['Turbine rating MW'] * 1000
    bos.rename(columns={'Rotor diameter m': 'Rotor Diam [m]', 'Cost per project': 'BOS Capex [USD]'}, inplace=True)
    bos.drop(columns=['Turbine rating MW'], inplace=True)

    # Aggregate and sum BOS costs
    print('Aggregating BOS costs...')
    bos_sum = bos.groupby(['Rating [kW]', 'Rotor Diam [m]', 'Number of turbines', 'Hub height m', 'Labor cost multiplier', 'Crane breakdown fraction']).sum().reset_index()

    # Inner join AEP and TCC. Taken together, Rating [kW] and Rotor Diam [m]
    # are the key.
    print('Joining AEP and TCC...')
    aep_tcc = aep.merge(tcc, on=['Rating [kW]', 'Rotor Diam [m]'])

    if len(aep_tcc) == 0:
        raise Exception('aep_tcc merge is empty')

    # Then join in the BOS sum data, again using Rating [kW] and
    # Rotor Diam [m] as keys. This dataframe will eventually have the
    # LCOE as a column.
    print('Joining aep_tcc and bos_sum...')
    lcoe = aep_tcc.merge(bos_sum, on=['Rating [kW]', 'Rotor Diam [m]'])

    if len(bos_sum) == 0:
        raise Exception('bos_sum is empty.')

    print('Calculatig the LCOE...')
    # Create columns for FCR and Opex USD/kW
    lcoe['FCR'] = 0.079
    lcoe['Opex [USD/kW]'] = 52.0

    # Now calculate LCOE and save the intermediate columns
    lcoe['Total Opex [USD]'] = lcoe['Opex [USD/kW]'] * lcoe['Rating [kW]'] * lcoe['Number of turbines']
    lcoe['Turbine Capex [USD]'] = lcoe['TCC [USD/kW]'] * lcoe['Rating [kW]'] * lcoe['Number of turbines']
    capex_times_fcr = (lcoe['BOS Capex [USD]'] + lcoe['Turbine Capex [USD]']) * lcoe['FCR']
    aep_all_turbines = lcoe['AEP [kWh/yr]'] * lcoe['Number of turbines']
    lcoe['LCOE [USD/kWh]'] = (capex_times_fcr + lcoe['Total Opex [USD]']) / aep_all_turbines

    print(f'Writing LCOE analysis with {len(lcoe)} rows...')
    lcoe.to_csv('lcoe_analysis.csv', index=False)

    print('Done writing LCOE analysis.')
