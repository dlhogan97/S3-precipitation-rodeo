sos_ds = xr.open_dataset('./01_data/processed_data/sos/sos_ds_1H_storage.nc').sel(time=slice('2022-12-01', '2023-03-31'))

w23_sos_kp_SWE_p1 = sos_ds['SWE_p1_c']
# remove datapoints where diff is greater than - 30
w23_sos_kp_SWE_p1 = w23_sos_kp_SWE_p1.where(np.abs(w23_sos_kp_SWE_p1.diff('time')) <30, drop=True)
# substract by the first value
w23_sos_kp_SWE_p1 = w23_sos_kp_SWE_p1 - w23_sos_kp_SWE_p1[0]

w23_sos_kp_SWE_p2 = sos_ds['SWE_p2_c']
# remove datapoints where diff is greater than - 30
w23_sos_kp_SWE_p2 = w23_sos_kp_SWE_p2.where(np.abs(w23_sos_kp_SWE_p2.diff('time')) <30, drop=True)
# substract by the first value
w23_sos_kp_SWE_p2 = w23_sos_kp_SWE_p2 - w23_sos_kp_SWE_p2[0]

w23_sos_kp_SWE_p3 = sos_ds['SWE_p3_c']
# remove datapoints where diff is greater than - 30
w23_sos_kp_SWE_p3 = w23_sos_kp_SWE_p3.where(np.abs(w23_sos_kp_SWE_p3.diff('time')) <30, drop=True)
# substract by the first value
w23_sos_kp_SWE_p3 = w23_sos_kp_SWE_p3 - w23_sos_kp_SWE_p3[0]

w23_sos_kp_SWE_p4 = sos_ds['SWE_p4_c']
# remove datapoints where diff is greater than - 30
w23_sos_kp_SWE_p4 = w23_sos_kp_SWE_p4.where(np.abs(w23_sos_kp_SWE_p4.diff('time')) <30, drop=True)
# substract by the first value
w23_sos_kp_SWE_p4 = w23_sos_kp_SWE_p4 - w23_sos_kp_SWE_p4[0]

# for ds in [w23_sos_kp_SWE_p1, w23_sos_kp_SWE_p2, w23_sos_kp_SWE_p3, w23_sos_kp_SWE_p4]:
#     ds.plot()
