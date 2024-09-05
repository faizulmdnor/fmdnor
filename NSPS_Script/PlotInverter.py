from fleet_performance import Site

s = 'BGM1'
site = Site(s)
start_inv, stop_inv = '2023-10-06', '2023-10-07'
site.plot_inverters_output(start_inv, stop_inv, by_block=True, view=True, normalize_by_dc_capacity=False)