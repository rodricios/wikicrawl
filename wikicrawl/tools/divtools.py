__author__ = 'rodrigo'

def avg_jsd2(dists1, dists2):
    ttl_js_div = sum([jsd2(dist1, dists2.get(key, {}))
                      for key, dist1 in dists1.items()])
    return ttl_js_div / len(dists1)


def jsd2(dist1, dist2, debug=False):
    """Jensen-shannon divergence"""

    import warnings
    import numpy as np
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    x = []
    y = []
    if len(dist1) < len(dist2):
        for key, val in dist2.items():
            x.append(dist1.get(key, 0))
            y.append(val)

    elif len(dist1) >= len(dist2):

        for key, val in dist1.items():
            x.append(val)
            y.append(dist2.get(key, 0))

    if debug:
        print('x:', x, 'y:', y)

    x = np.array(x)
    y = np.array(y)

    d1 = x * np.log2(2 * x / (x + y))
    d2 = y * np.log2(2 * y / (x + y))

    d1[np.isnan(d1)] = 0
    d2[np.isnan(d2)] = 0

    d = 0.5 * np.sum(d1 + d2)

    return d


def get_cosine(vec1, vec2):
    import math

    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator
