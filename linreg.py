import numpy as np
from sklearn.linear_model import LinearRegression

dt = np.dtype([('block', '<u4'), ('best', '<u4'), ('fastlz', '<u4'), ('zeroes', '<u4'), ('ones', '<u4')])
input = np.fromfile('./fastlz.bin.op', dtype=dt)
input_array = np.array(input.tolist())

x = np.delete(input_array, [0,1], 1)
y = input_array[:, 1]
fastlz_model = LinearRegression().fit(x, y)

x_zeros_ones_combined = np.copy(x)
x_zeros_ones_combined[:, 1] += x_zeros_ones_combined[:, 2]
x_zeros_ones_combined = np.delete(x_zeros_ones_combined, [2], 1)
fastlz_combined_model = LinearRegression().fit(x_zeros_ones_combined, y)

x_simple = np.delete(x, [1,2], 1)
fastlz_simple_model = LinearRegression().fit(x_simple, y)

x_naive = np.delete(x, [0], 1)
naive_model = LinearRegression().fit(x_naive, y)

blocks_per_day = 30*60*24
grouped = input_array // [blocks_per_day, 1, 1, 1, 1]
sorted = grouped[grouped[:, 0].argsort()]
split = np.split(sorted, np.unique(sorted[:, 0], return_index=True)[1][1:])

naive_scalar = np.sum(y) / (np.sum(x[:, 1]*4+x[:, 2]*16)/16)
fastlz_scalar = np.sum(y) / np.sum(x[:, 0])

print(f'naive_scalar: {naive_scalar}')
print(f'fastlz_scalar: {fastlz_scalar}')

print(f'block,rms,rms_combined,rms_simple,rms_naive,rms_naive_cheap,rms_fastlz_cheap,ma,ma_combined,ma_simple,ma_naive,ma_naive_cheap,ma_fastlz_cheap')
for day in split:
    xd = np.delete(day, [0,1], 1)
    yd = day[:, 1]
    rms = np.sqrt(np.sum(np.power(fastlz_model.predict(xd) - yd, 2)) / np.size(yd))
    ma = np.sum(np.absolute(fastlz_model.predict(xd) - yd)) / np.size(yd)

    xd_zeros_ones_combined = np.copy(xd)
    xd_zeros_ones_combined[:, 1] += xd_zeros_ones_combined[:, 2]
    xd_zeros_ones_combined = np.delete(xd_zeros_ones_combined, [2], 1)
    rms_combined = np.sqrt(np.sum(np.power(fastlz_combined_model.predict(xd_zeros_ones_combined) - yd, 2)) / np.size(yd))
    ma_combined = np.sum(np.absolute(fastlz_combined_model.predict(xd_zeros_ones_combined) - yd)) / np.size(yd)

    xd_simple = np.delete(xd, [1,2], 1)
    rms_simple = np.sqrt(np.sum(np.power(fastlz_simple_model.predict(xd_simple) - yd, 2)) / np.size(yd))
    ma_simple = np.sum(np.absolute(fastlz_simple_model.predict(xd_simple) - yd)) / np.size(yd)

    xd_naive = np.delete(xd, [0], 1)
    rms_naive = np.sqrt(np.sum(np.power(naive_model.predict(xd_naive) - yd, 2)) / np.size(yd))
    ma_naive = np.sum(np.absolute(naive_model.predict(xd_naive) - yd)) / np.size(yd)

    rms_naive_cheap = np.sqrt(np.sum(np.power(yd - (xd[:, 1]*4+xd[:, 2]*16)/16*naive_scalar, 2)) / np.size(yd))
    ma_naive_cheap = np.sum(np.absolute(yd - (xd[:, 1]*4+xd[:, 2]*16)/16*naive_scalar)) / np.size(yd)
    rms_fastlz_cheap = np.sqrt(np.sum(np.power(yd - xd[:, 0]*fastlz_scalar, 2)) / np.size(yd))
    ma_fastlz_cheap = np.sum(np.absolute(yd - xd[:, 0]*fastlz_scalar)) / np.size(yd)

    print(f'{day[0][0]},{rms:.2f},{rms_combined:.2f},{rms_simple:.2f},{rms_naive:.2f},{rms_naive_cheap:.2f},{rms_fastlz_cheap:.2f},{ma:.2f},{ma_combined:.2f},{ma_simple:.2f},{ma_naive:.2f},{ma_naive_cheap:.2f},{ma_fastlz_cheap:.2f}')
