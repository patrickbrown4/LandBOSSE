"""
BOSModel.py
Created by Annika Eberle and Owen Roberts on Feb. 28, 2018

Calculates the following balance of system costs for land-based wind projects:

- Development (currently input by the user)
- Management
- Roads
- Foundations
- Erection
- Collection system
- Transmission and interconnection
"""

import WeatherDelay as WD
import ErectionCost
import ManagementCost
import FoundationCost
import RoadsCost
from itertools import product
import pandas as pd
import numpy as np

# constants
kilowatt_per_megawatt = 1000
development_cost = 5e6  # value input by the user (generally ranges from $3-10 million)
per_diem = 144  # USD per day
season_construct = ['spring', 'summer']
time_construct = 'normal'
construction_time_months = 13
num_hwy_permits = 1  # assuming number of highway permits = 1

# financial parameters
markup_constants = {'contingency': 0.03,
                    'warranty_management': 0.0002,
                    'sales_and_use_tax': 0,
                    'overhead': 0.05,
                    'profit_margin': 0.05}

# dictionary of seasons
season_dict = {'winter':   [12, 1, 2],
               'spring':   [3, 4, 5],
               'summer':   [6, 7, 8],
               'fall':     [9, 10, 11]}

operational_hour_dict = {'long': 24,
                         'normal': 10}

phase_list = {'Collection system': '',
              'Development': '',
              'Erection': '',
              'Management': '',
              'Foundations': '',
              'Roads': '',
              'Transmission and interconnection': '',
              'Substation': ''}

type_of_cost = ['Labor', 'Equipment rental', 'Mobilization', 'Fuel', 'Materials', 'Development', 'Management', 'Other']


def calculate_bos_cost(files, scenario_name, scenario_height, development):
    """
    Executes the calculate costs functions for each module/phase in the balance of system

    :param files: [dict] dictionary of files with input data from the user
    :param scenario_name: [str] name of scenario to be run (must be in project file)
    :param scenario_height: [str] hub height of scenario to be run (must be in project file)
    :param development: [float] development costs input by the user
    :return: total BOS costs for by phase and type; weather delay by phase
    """

    print("Running LandBOSSE...")
    # read csv files and load data into dictionary
    data_csv = dict()
    for file in files:
        data_csv[file] = pd.DataFrame(pd.read_csv(files[file], engine='python'))

    # extract project parameters from input data
    project_data = data_csv['project'].where((data_csv['project']['Project ID'] == scenario_name) & (data_csv['project']['Hub height m'] == scenario_height))
    project_data = project_data.dropna(thresh=1)
    num_turbines = float(project_data['Number of turbines'])
    turbine_spacing = float(project_data['Turbine spacing (times rotor diameter)'])
    rotor_diameter = float(project_data['Rotor diameter m'])
    turbine_rating_kilowatt = float(project_data['Turbine rating MW']) * kilowatt_per_megawatt
    rate_of_deliveries = float(project_data['Rate of deliveries (turbines per week)'])
    hub_height = float(project_data['Hub height m'])
    wind_shear_exponent = float(project_data['Wind shear exponent'])
    tower_type = project_data['Tower type'].values[0]
    foundation_depth = float(project_data['Foundation depth m'])
    project_size = num_turbines * turbine_rating_kilowatt / kilowatt_per_megawatt  # project size in megawatts

    # default electrical parameters
    interconnect_voltage = 137
    pad_mount_transformer = True
    MV_thermal_backfill_mi = 0
    MV_overhead_collector_mi = 0
    rock_trenching_percent = 0.1
    distance_to_interconnect = 5
    new_switchyard = True

    # default road parameters
    road_length_m = ((np.sqrt(num_turbines) - 1) ** 2 * turbine_spacing * rotor_diameter)
    road_width_ft = 20  # feet 16
    road_thickness_in = 8  # inches
    crane_width_m = 12.2  # meters 10.7
    num_access_roads = 5

    # default labor parameters
    overtime_multiplier = 1.4  # multiplier for labor overtime rates due to working 60 hr/wk rather than 40 hr/wk

    # create data frame to store cost data for each module
    bos_cost = pd.DataFrame(list(product(phase_list, type_of_cost)), columns=['Phase of construction', 'Type of cost'])
    bos_cost['Cost USD'] = np.nan

    # create weather window for project
    weather_window = WD.create_weather_window(weather_data=data_csv['weather'],
                                              season_id=season_dict,
                                              season_construct=season_construct,
                                              time_construct=time_construct)

    operational_hrs_per_day = operational_hour_dict[time_construct]

    # calculate road costs
    [road_cost, road_wind_mult] = RoadsCost.calculate_costs(road_length=road_length_m,
                                                            road_width=road_width_ft,
                                                            road_thickness=road_thickness_in,
                                                            input_data=data_csv,
                                                            construction_time=construction_time_months,
                                                            weather_window=weather_window,
                                                            crane_width_m=crane_width_m,
                                                            operational_hrs_per_day=operational_hrs_per_day,
                                                            num_turbines=num_turbines,
                                                            rotor_diam=rotor_diameter,
                                                            access_roads=num_access_roads,
                                                            per_diem_rate=per_diem,
                                                            overtime_multiplier=overtime_multiplier,
                                                            wind_shear_exponent=wind_shear_exponent
                                                            )

    # calculate foundation costs
    [foundation_cost, foundation_wind_mult] = FoundationCost.calculate_costs(input_data=data_csv,
                                                                             num_turbines=num_turbines,
                                                                             construction_time=construction_time_months,
                                                                             weather_window=weather_window,
                                                                             operational_hrs_per_day=operational_hrs_per_day,
                                                                             overtime_multiplier=overtime_multiplier,
                                                                             wind_shear_exponent=wind_shear_exponent,
                                                                             tower_type=tower_type,
                                                                             depth=foundation_depth)

    # set values in bos_cost data frame - since formatting is already correct for foundation_cost, then overwrite values
    for value in foundation_cost['Type of cost']:
        bos_cost.loc[(bos_cost['Phase of construction'] == 'Foundations') &
                     (bos_cost['Type of cost'] == value), ['Cost USD']] = foundation_cost.loc[
        (foundation_cost['Phase of construction'] == 'Foundations') &
        (foundation_cost['Type of cost'] == value), ['Cost USD']].values

    # set values in bos_cost data frame - since formatting is already correct for road_cost, then overwrite values
    for value in road_cost['Type of cost']:
        bos_cost.loc[(bos_cost['Phase of construction'] == 'Roads') &
                     (bos_cost['Type of cost'] == value), ['Cost USD']] = road_cost.loc[
        (road_cost['Phase of construction'] == 'Roads') &
        (road_cost['Type of cost'] == value), ['Cost USD']].values

    # todo: move electrical calculations to separate modules
    substation = 11652 * (interconnect_voltage + project_size) + 11795 * (project_size ** 0.3549) + 1526800

    if distance_to_interconnect == 0:
        trans_interconnect = 0
    else:
        if new_switchyard is True:
            interconnect_adder = 18115 * interconnect_voltage + 165944
        else:
            interconnect_adder = 0
        trans_interconnect = ((1176 * interconnect_voltage + 218257) * (distance_to_interconnect ** (-0.1063))
                              * distance_to_interconnect) + interconnect_adder

    if pad_mount_transformer is True:
        multipier_material = 66733.4
    else:
        multipier_material = 27088.4
    electrical_materials = num_turbines * multipier_material + int(project_size / 25) * 35375 + \
                         int(project_size / 100) * 50000 + rotor_diameter * num_turbines * 545.4 + \
                         MV_thermal_backfill_mi * 5 + 41945
    if project_size > 200:
        material_adder = 300000
    else:
        material_adder = 155000
    electrical_installation = int(project_size / 25) * 14985 + material_adder + \
                           num_turbines * (7059.3 + rotor_diameter * (352.4 + 297 * rock_trenching_percent)) + \
                           MV_overhead_collector_mi * 200000 + 10000

    bos_cost.loc[(bos_cost['Phase of construction'] == 'Transmission and interconnection') &
                 (bos_cost['Type of cost'] == 'Other'), ['Cost USD']] = trans_interconnect

    bos_cost.loc[(bos_cost['Phase of construction'] == 'Substation') &
                 (bos_cost['Type of cost'] == 'Other'), ['Cost USD']] = substation

    bos_cost.loc[(bos_cost['Phase of construction'] == 'Collection system') &
                 (bos_cost['Type of cost'] == 'Other'), ['Cost USD']] = electrical_installation + electrical_materials

    # calculate erection costs
    erection_cost = ErectionCost.calculate_costs(project_specs=project_data,
                                                 project_data=data_csv,
                                                 hour_day=operational_hour_dict,
                                                 construction_time=construction_time_months,
                                                 time_construct=time_construct,
                                                 weather_window=weather_window,
                                                 rate_of_deliveries=rate_of_deliveries,
                                                 overtime_multiplier=overtime_multiplier,
                                                 project_size=project_size,
                                                 wind_shear_exponent=wind_shear_exponent
                                                 )

    bos_cost = save_cost_data(phase='Erection',
                              phase_cost=erection_cost,
                              bos_cost=bos_cost)

    bos_cost = save_cost_data(phase='Development',
                              phase_cost=pd.DataFrame([[development]], columns=['Development cost USD']),
                              bos_cost=bos_cost)

    project_value = float(bos_cost['Cost USD'].sum())

    # calculate management costs
    management_cost = ManagementCost.calculate_costs(project_value=project_value,
                                                     foundation_cost=foundation_cost,
                                                     num_hwy_permits=num_hwy_permits,
                                                     construction_time_months=construction_time_months,
                                                     markup_constants=markup_constants,
                                                     num_turbines=num_turbines,
                                                     project_size=project_size,
                                                     hub_height=hub_height,
                                                     num_access_roads=num_access_roads)

    bos_cost = save_cost_data(phase='Management',
                              phase_cost=management_cost,
                              bos_cost=bos_cost)

    # weather delays
    erection_wind_mult = (erection_cost['Total time per op with weather']) / (erection_cost['Total time per op with weather'] - erection_cost['Time weather'])
    wind_multiplier = pd.DataFrame([['Erection', erection_wind_mult.reset_index(drop=True).mean()],
                                    ['Foundation', foundation_wind_mult],
                                    ['Road', road_wind_mult]], columns=['Operation', 'Wind multiplier'])

    print('Final cost matrix:')
    print(bos_cost)

    print('Total cost by phase:')
    print(bos_cost.groupby(by=bos_cost['Phase of construction']).sum())

    print('\n Total cost USD:')
    print(bos_cost['Cost USD'].sum())

    print(wind_multiplier)

    return bos_cost, wind_multiplier, road_length_m, num_turbines, project_size


def save_cost_data(phase, phase_cost, bos_cost):
    """
    Saves phase-specific cost data to a data frame for total BOS costs

    :param phase: string that describes the phase of interest
    :param phase_cost: data frame with costs for phase of interest
    :param bos_cost: data frame of BOS costs
    :return: updated data frame with BOS costs for phase of interest
    """

    for index in bos_cost[bos_cost['Phase of construction'] == phase]['Type of cost'].index:
        if '{type} cost USD'.format(type=bos_cost['Type of cost'][index]) in phase_cost.columns:
            bos_cost.loc[index, 'Cost USD'] = phase_cost['{type} cost USD'.format(type=bos_cost['Type of cost'][index])].sum()

    return bos_cost


if __name__ == '__main__':

    scenario_list = list(["Steel - 2.3", "Steel - BAU 5", "Steel - DNV 4.5", "Steel - DNV 6.5", "Steel - High SP 5"])
    scenario_height = list([90, 117, 130, 160, 110])

    scenario_data_compiled = pd.DataFrame(columns=["Scenario", "Phase of construction", "Cost USD"])
    other_scenario_data_compiled = pd.DataFrame(columns=["Scenario", "Parameter", "Value"])


    for i in range(0, len(scenario_list)):
        scenario = scenario_list[i]
        height = scenario_height[i]
        print(i)
        print(scenario)
        print(height)
        # model inputs
        # todo: replace with function call for user input
        # dictionary of file names for input data
        dir_path = "/Users/aeberle/Desktop/BAR_analysis/"
        file_path = dir_path #+ "/default_inputs_for_model_release/"
        file_list = {'crane_specs':     file_path + "crane_specs.csv",
                     'equip':           file_path + "equip.csv",
                     'crew':            file_path + "crews.csv",
                     'components':      file_path + scenario + ".csv",
                     'project':         file_path + "project_scenario_list_bar_100turbine.csv",
                     'equip_price':     file_path + "equip_price.csv",
                     'crew_price':      file_path + "crew_price.csv",
                     'material_price':  file_path + "material_price.csv",
                     'weather':         file_path + "weather_withtime.csv",
                     'rsmeans':         file_path + "rsmeans_data.csv"}

        [bos_cost_1, wind_mult_1, road_length, num_turbines, project_size] = calculate_bos_cost(files=file_list,
                                                       scenario_name=scenario,
                                                       scenario_height=height,
                                                       development=development_cost)

        sum_bos = bos_cost_1.groupby(by="Phase of construction").sum()
        scenario_data = pd.DataFrame(columns=["Scenario", "Phase of construction", "Cost USD"])
        scenario_data['Scenario'] = ([scenario] * 8)
        scenario_data['Phase of construction'] = sum_bos.index.values.tolist()
        scenario_data['Cost USD'] = sum_bos['Cost USD'].values.tolist()
        scenario_data_compiled = scenario_data_compiled.append(scenario_data)

        scenario_sum = pd.DataFrame(columns=["Scenario", "Phase of construction", "Cost USD"])
        scenario_sum['Scenario'] = [scenario] * 3
        scenario_sum['Phase of construction'] = ['Total', 'Total per turbine', 'Total per MW']
        scenario_sum['Cost USD'] = [scenario_data['Cost USD'].sum(), scenario_data['Cost USD'].sum()/num_turbines, scenario_data['Cost USD'].sum()/project_size]
        scenario_data_compiled = scenario_data_compiled.append(scenario_sum)

        scenario_weather = pd.DataFrame(columns=["Scenario", "Parameter", "Value"])
        scenario_weather['Scenario'] = [scenario] * 2
        scenario_weather['Parameter'] = ['Wind delay multiplier', 'Road length m']
        scenario_weather['Value'] = [wind_mult_1.iloc[0]['Wind multiplier'], road_length]
        other_scenario_data_compiled = other_scenario_data_compiled.append(scenario_weather)


        scenario_data_compiled.to_csv('/Users/aeberle/Desktop/BAR_analysis/output_100turbine.csv')
        other_scenario_data_compiled.to_csv('/Users/aeberle/Desktop/BAR_analysis/output_other_params_100turbine.csv')