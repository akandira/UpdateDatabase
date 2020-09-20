"""
This script contains functions to visualize database data for X-materials

@author: AK
"""
# import libraries
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import configparser
from IPython.display import display_html
from plotly.subplots import make_subplots


def get_sql_conn():
    """Setup PostgreSQL DB connection"""

    # get config information
    config = configparser.ConfigParser()
    config.sections()
    config.read('../config.ini')
    dbname = config['PostgresDB']['db_name']
    host = config['PostgresDB']['host']
    port = config['PostgresDB']['port']
    user = config['PostgresDB']['user']
    pw = config['PostgresDB']['pw']

    # connect to the database
    conn = psycopg2.connect(host=host, port=port, database=dbname,
                            user=user, password=pw)
    return conn


def mat_section(ball_id='MATX-BM005'):
    """Return material properties of unique ball-mill id"""

    try:

        # get connection
        conn = get_sql_conn()

        # intialize variables
        hot_id = ''
        ball_out_id = ''
        hot_out_id = ''

        # query material procurement table for given id value
        query_mat = \
            'Select * from material_procurement where ball_milling_uid = \'{}\''.format(str(ball_id))
        df_mat = pd.read_sql_query(query_mat, con=conn)

        # query ball mill table for given id value
        query_ball = \
            'Select * from ball_milling where uid = \'{}\''.format(ball_id)
        df_ball = pd.read_sql_query(query_ball, con=conn)

        # if valid entry for a material exists, get output material id and hot press id
        if not df_ball.empty:
            ball_out_id = df_ball.iloc[0]['output_material_uid']
            hot_id = df_ball.iloc[0]['hot_press_uid']

        # if valid hot press id exsists query hot press table
        if hot_id:
            query_hot = \
                'Select * from hot_press where uid = \'{}\''.format(hot_id)
            df_hot = pd.read_sql_query(query_hot, con=conn)

        # get output material id from hot table
        if not df_hot.empty:
            hot_out_id = df_hot.iloc[0]['output_material_uid']

        # get lab reports for ball mill and hot press material from hall measurement and icp measurement tables
        if ball_out_id or hot_out_id:

            query_hall = \
                'Select * from hall_measurement where material_uid in (\'{}\', \'{}\')'.format(hot_out_id,
                    ball_out_id)
            df_hall = pd.read_sql_query(query_hall, con=conn)

            query_icp = \
                'Select * from icp_measurement where material_uid in (\'{}\', \'{}\')'.format(hot_out_id,
                    ball_out_id)
            df_icp = pd.read_sql_query(query_icp, con=conn)

        # format materials table
        df_mat = df_mat.drop(['uid', 'ball_milling_uid'], axis=1)

        # format ball mill table
        df_ball['milling_speed'] = df_ball['milling_speed'].astype(str) \
            + ' ' + df_ball['milling_speed_units']
        df_ball['milling_time'] = df_ball['milling_time'].astype(str) \
            + ' ' + df_ball['milling_time_units']
        df_ball = df_ball[['milling_speed', 'milling_time']]
        df_ball = df_ball.T.reset_index()
        df_ball = df_ball.rename(columns={'index': 'BALL MILLING', 0: ''
                                 })

        # format hot press table
        df_hot['hot_press_temperature'] = df_hot['hot_press_temperature'
                ].astype(str) + ' ' \
            + df_hot['hot_press_temperature_units']
        df_hot['hot_press_pressure'] = df_hot['hot_press_pressure'
                ].astype(str) + ' ' + df_hot['hot_press_pressure_units']
        df_hot['hot_press_time'] = df_hot['hot_press_time'].astype(str) \
            + ' ' + df_hot['hot_press_time_units']
        df_hot = df_hot[['hot_press_temperature', 'hot_press_pressure',
                        'hot_press_time', 'output_material_name']]
        df_hot = df_hot.T.reset_index()
        df_hot = df_hot.rename(columns={'index': 'HOT PROCESS', 0: ''})

        # format hall measurement table
        df_hall['probe_resistance'] = df_hall['probe_resistance'
                ].astype(str) + ' ' + df_hall['probe_resistance_units']
        df_hall['current'] = df_hall['current'].astype(str) + ' ' \
            + df_hall['current_units']
        df_hall['field_strength'] = df_hall['field_strength'
                ].astype(str) + ' ' + df_hall['field_strength_units']
        df_hall = df_hall[['process_type', 'probe_resistance',
                          'probe_material', 'current', 'field_strength'
                          ]]
        df_hall = df_hall.T.reset_index()
        df_hall = df_hall.rename(columns={'index': 'HALL REPORT', 0: ''
                                 , 1: ''})

        # format icp measurement table
        df_icp['gas_flow_rate'] = df_icp['gas_flow_rate'].astype(str) \
            + ' ' + df_icp['gas_flow_rate_units']
        df_icp['radio_frequency'] = df_icp['radio_frequency'
                ].astype(str) + ' ' + df_icp['radio_frequency_units']
        df_icp = df_icp[[
            'process_type',
            'pb_concentration',
            'sn_concentration',
            'o_concentration',
            'gas_flow_rate',
            'radio_frequency',
            ]]
        df_icp = df_icp.T.reset_index()
        df_icp = df_icp.rename(columns={'index': 'HALL REPORT', 0: '',
                               1: ''})

        # create a side by side display of tables with a heading
        display_html('<h1> Material Properties {} </h1>'.format(str(ball_id)),
                     raw=True)

        # function to print tables side by side
        display_side_by_side(df_mat, df_ball, df_hot, df_hall, df_icp)

        # close opened database connection
        conn.close()
    except Exception:
        print ('error occured: Please check Ball Mill ID')


def display_side_by_side(*args):
    """Print Pandas dataframes side by side in python notebook"""

    html_string = ''
    for df in args:
        html_string += df.to_html(index=False, header=True)
    display_html(html_string.replace('table',
                 'table style="display:inline"'), raw=True)


def merge_tables():
    """Join all the tables in dataframe"""

    # get sql connection
    conn = get_sql_conn()

    # get all info from materials table
    query_mat = 'Select * from material_procurement'
    df_mat = pd.read_sql_query(query_mat, con=conn)
    df_mat = df_mat.drop(['uid'], axis=1)
    df_mat = df_mat.pivot(index='ball_milling_uid',
                          columns='material_name',
                          values='mass_fraction')
    df_mat = df_mat.reset_index()
    df_mat = df_mat.add_prefix('MT-')

    # get all info from ball mill table
    query_ball = 'Select * from ball_milling'
    df_ball = pd.read_sql_query(query_ball, con=conn)

    # added prefix to distinctly identify a column
    df_ball = df_ball.add_prefix('BM-')

    # get all info from hot process
    query_hot = 'Select * from hot_press'
    df_hot = pd.read_sql_query(query_hot, con=conn)

    # added prefix to distinctly identify a column
    df_hot = df_hot.add_prefix('HP-')

    # get all info from hall measurements table
    query_hall = 'Select * from hall_measurement'
    df_hall = pd.read_sql_query(query_hall, con=conn)

    # get all info from icp measurements table
    query_icp = 'Select * from icp_measurement'
    df_icp = pd.read_sql_query(query_icp, con=conn)

    # Left merge tables in database starting from materials area to lab reports
    df_com = df_ball.merge(df_mat, how='left', left_on='BM-uid',
                           right_on='MT-ball_milling_uid')
    df_com = df_com.merge(df_hot, how='left', left_on='BM-hot_press_uid'
                          , right_on='HP-uid')
    df_com = df_com.merge(df_hall.add_prefix('BM-HA-'), how='left',
                          left_on='BM-output_material_uid',
                          right_on='BM-HA-material_uid')
    df_com = df_com.merge(df_icp.add_prefix('BM-ICP-'), how='left',
                          left_on='BM-output_material_uid',
                          right_on='BM-ICP-material_uid')
    df_com = df_com.merge(df_hall.add_prefix('HP-HA-'), how='left',
                          left_on='HP-output_material_uid',
                          right_on='HP-HA-material_uid')
    df_com = df_com.merge(df_icp.add_prefix('HP-ICP-'), how='left',
                          left_on='HP-output_material_uid',
                          right_on='HP-ICP-material_uid')

    # close connection
    conn.close()

    # return complete db tables
    return df_com


def compare_materials(supList=['MATX-BM001', 'MATX-BM002', 'MATX-BM003'
                      ]):
    """Compare multiple material properties against each other
       One can supply as many material IDs as they want, default is 3"""

   # check to catch any errors
    try:

       # get complete merged table
        df_com = merge_tables()

       # filter for required fields
        df_filtered = df_com[df_com['BM-uid'].isin(supList)]

       # format table
        df_filtered = df_filtered.T
        df_filtered = df_filtered.dropna()
    except Exception:

       # if exception arises notify users
        print ('error occured: Please check Ball Mill IDs')

    return df_filtered


def getFigure():
    """Plot quantifiable data in a subplot grid using Plotly's
       interactive plotting tools"""

    # get merged tables
    df_com = merge_tables()

    # list of selected parameters
    selectedlist = [
        'MT-Cu',
        'MT-Se',
        'MT-Zn',
        'BM-milling_time',
        'BM-milling_speed',
        'HP-hot_press_temperature',
        'HP-hot_press_pressure',
        'HP-hot_press_time',
        'BM-HA-probe_resistance',
        'HP-HA-probe_resistance',
        'HP-ICP-radio_frequency',
        'BM-ICP-radio_frequency',
        'BM-ICP-pb_concentration',
        'BM-ICP-sn_concentration',
        'BM-ICP-o_concentration',
        'HP-ICP-pb_concentration',
        'HP-ICP-sn_concentration',
        'HP-ICP-o_concentration',
        ]

    # format data frame
    df_com = df_com.reset_index()

    # x axis for plot
    x_col = 'index'

    # plotly figure layout options
    fig = make_subplots(rows=5, cols=6, specs=[
        [{'colspan': 6},None,None,None,None,None,],
        [{'rowspan': 1, 'colspan': 3}, None,None, {'rowspan': 1, 'colspan': 3},None,None,], 
        [{'colspan': 2},None,{'colspan': 2},None,{'colspan': 2},None,],
        [{'rowspan': 1, 'colspan': 3}, None,None,{'rowspan': 1, 'colspan': 3},None, None,],
        [{'rowspan': 1, 'colspan': 3},None,None,{'rowspan': 1, 'colspan': 3},None,None,]], horizontal_spacing=0.1)

    # Materials table data plotting
    y_col1 = selectedlist[0]
    y_col2 = selectedlist[1]
    y_col3 = selectedlist[2]
    fig.add_trace(go.Bar(x=df_com[x_col], y=df_com[y_col1],
                  marker_color='rgb(55, 83, 109)', name='Cu'), row=1,
                  col=1)  # ,offsetgroup=0
    fig.add_trace(go.Bar(x=df_com[x_col], y=df_com[y_col2],
                  marker_color='rgb(26, 118, 255)', name='Se'), row=1,
                  col=1)
    fig.add_trace(go.Bar(x=df_com[x_col], y=df_com[y_col3],
                  marker_color='rgb(0,191,255)', name='Zn'), row=1,
                  col=1)
    fig.update_xaxes(title_text='Index', showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Raw Material Composition',
                     showgrid=False, row=1, col=1)

    # Ball milling table data plotting
    y_col = selectedlist[3]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['BM-milling_time_units'], df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_size=7,
        marker_color='rgb(0,191,255)',
        marker_line_width=1,
        name='',
        ), row=2, col=1)
    fig.update_xaxes(title_text='Index', row=2, col=1)
    fig.update_yaxes(title_text='Ball Milling Time', row=2, col=1)

    y_col = selectedlist[4]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['BM-milling_speed_units'], df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(0,191,255)',
        marker_line_width=1,
        marker_size=7,
        name='',
        ), row=2, col=4)
    fig.update_xaxes(title_text='Index', row=2, col=4)
    fig.update_yaxes(title_text='Ball Milling Speed', row=2, col=4)

    # Hot process table data plotting
    y_col = selectedlist[5]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='square',
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['HP-hot_press_temperature_units'],
              df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(55, 83, 109)',
        marker_line_width=1,
        marker_size=7,
        name='',
        ), row=3, col=1)
    fig.update_xaxes(title_text='Index', row=3, col=1)
    fig.update_yaxes(title_text='Hot Press Temperature', row=3, col=1)

    y_col = selectedlist[6]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='square',
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['HP-hot_press_pressure_units'], df_com['BM-uid'
              ])],
        marker_line_color='midnightblue',
        marker_color='rgb(55, 83, 109)',
        marker_line_width=1,
        marker_size=7,
        name='',
        ), row=3, col=3)
    fig.update_xaxes(title_text='Index', row=3, col=3)
    fig.update_yaxes(title_text='Hot Press Pressure', row=3, col=3)


    y_col = selectedlist[7]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='square',
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['HP-hot_press_time_units'], df_com['BM-uid'
              ])],
        marker_line_color='midnightblue',
        marker_color='rgb(55, 83, 109)',
        marker_line_width=1,
        marker_size=7,
        name='',
        ), row=3, col=5)
    fig.update_xaxes(title_text='Index', row=3, col=5)
    fig.update_yaxes(title_text='Hot Press Time', row=3, col=5)

    # Hall measurement table plots
    y_col = selectedlist[8]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['BM-HA-probe_resistance_units'],
              df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(35,54,183)',
        marker_line_width=1,
        marker_size=7,
        name='Ball Mill',
        ), row=4, col=1)
    fig.update_xaxes(title_text='Index', row=4, col=1)
    fig.update_yaxes(title_text='Probe Resistance', row=4, col=1)

    y_col = selectedlist[9]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='diamond',
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{}<br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['HP-HA-probe_resistance_units'],
              df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(274,94,91)',
        marker_line_width=1,
        marker_size=7,
        name='Hot Process',
        ), row=4, col=1)
    
    # ICP measurement table plots
    y_col = selectedlist[10]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['BM-ICP-radio_frequency_units'],
              df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(35,54,183)',
        marker_line_width=1,
        marker_size=7,
        name='Ball Mill',
        ), row=4, col=4)
    fig.update_xaxes(title_text='Index', row=4, col=4)
    fig.update_yaxes(title_text='ICP Radio Frequency', row=4, col=4)

    y_col = selectedlist[11]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='diamond',
        mode='markers',
        hovertemplate='<br>x: %{x}<br>' + 'y: %{y}' + '%{text}',
        text=['{} <br>{}<br>'.format(i, j) for (i, j) in
              zip(df_com['HP-ICP-radio_frequency_units'],
              df_com['BM-uid'])],
        marker_line_color='midnightblue',
        marker_color='rgb(274,94,91)',
        marker_line_width=1,
        marker_size=7,
        name='Hot Process',
        ), row=4, col=4)
    
    # ICP measurement table concentration plots
    y_col = selectedlist[12]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='square',
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='pb',
        marker_line_color='midnightblue',
        marker_color='red',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=1)
    fig.update_xaxes(title_text='Index', row=5, col=1)
    fig.update_yaxes(title_text='Ball Mill-ICP Concentration', row=5,
                     col=1)

    y_col = selectedlist[13]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='diamond',
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='sn',
        marker_line_color='midnightblue',
        marker_color='green',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=1)

    y_col = selectedlist[14]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='o',
        marker_line_color='midnightblue',
        marker_color='black',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=1)

    y_col = selectedlist[15]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='square',
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='pb',
        marker_line_color='midnightblue',
        marker_color='red',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=4)
    fig.update_xaxes(title_text='Index', row=5, col=4)
    fig.update_yaxes(title_text='Hot Press-ICP Concentration', row=5,
                     col=4)

    y_col = selectedlist[16]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        marker_symbol='diamond',
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='sn',
        marker_line_color='midnightblue',
        marker_color='green',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=4)

    y_col = selectedlist[17]
    fig.add_trace(go.Scatter(
        x=df_com[x_col],
        y=df_com[y_col],
        mode='markers',
        hovertemplate='y: %{y}' + '<br>x: %{x}<br>' + '%{text}',
        text=['<br>{}<br>'.format(i) for i in df_com['BM-uid']],
        name='o',
        marker_line_color='midnightblue',
        marker_color='black',
        marker_line_width=1,
        marker_size=7,
        ), row=5, col=4)

    # update figure options
    fig.update_layout(height=1800, width=900,
                      title_text='Mat X Summary', showlegend=False)

    # return figure with subplots
    return fig
