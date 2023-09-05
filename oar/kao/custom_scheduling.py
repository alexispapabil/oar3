from procset import ProcSet

import oar.kao.scheduling
from oar.lib import get_logger
from oar.lib.hierarchy import find_resource_hierarchies_scattered,extract_n_scattered_block_itv

logger = get_logger("oar.kamelot")

'''
Find the path leading to a resource within the hierarchy.
e.g. hy = {'nodes': [ProcSet((0, 7)), ProcSet((8, 15))], 'cpu': [ProcSet((0, 3)), ProcSet((4, 7)), ProcSet((8, 11)), ProcSet((12, 15))], 'core': [ProcSet(0), ProcSet(1), ProcSet(2), ProcSet(3), ProcSet(4), ProcSet(5), ProcSet(6), ProcSet(7), ProcSet(8), ProcSet(9), ProcSet(10), ProcSet(11), ProcSet(12), ProcSet(13), ProcSet(14), ProcSet(15)], 'resource_id': [ProcSet(0), ProcSet(1), ProcSet(2), ProcSet(3), ProcSet(4), ProcSet(5), ProcSet(6), ProcSet(7), ProcSet(8), ProcSet(9), ProcSet(10), ProcSet(11), ProcSet(12), ProcSet(13), ProcSet(14), ProcSet(15)]}
e.g. itvs = ProcSet((8,11))
path(itvs, hy) = [ProcSet((8,15)), ProcSet((8,11))]
'''
def path(itvs, hy):
    acc = []
    for lv in hy:
        for rsc in hy[lv]:
            if len(itvs) <= len(rsc):
                if itvs.issubset(rsc):
                    acc.append(rsc)
                    break
            else:
                break
    return sorted(acc, key = lambda x: len(x), reverse = True)

def compact(itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots
        # Select unused resources first (top-down). 
        hy_levels = map(lambda x: sorted(x, key = lambda i: [len(prev & itvs_cts_slots) for prev in path(i,hy)], reverse = True), hy_levels)
        res = find_resource_hierarchies_scattered(itvs_cts_slots, list(hy_levels), hy_nbs)
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


#def update(init, cur, i, bottom):
    #j = i + 1

    #if cur[i+1] == 0:
        #cur[i+1] = init[i+1]
        #cur[i] = 0 if cur[i] < 0 else cur[i]-1

    #elif cur[j] < 0:
        #if cur[i] > 0:
            #while j <= bottom and cur[j] <= 0:
                #j += 1
            #if j > bottom:
                #cur[i] -= 1
            #else:
                #return 1

    #return 0


def pick_n(avail, hier, rq, i, policy):
    j = 0
    acc = ProcSet()
    l = len(hier)

    cap = l//2 if policy == 'spread' else l

    while j < l and rq[i] != 0 and cap > 0:
        r = hier[j]
        if r & avail == r:
            cap -= 1
            acc = acc | r
            if rq[i] > 0:
                rq[i] -= 1
        j += 1
    
    if rq[i] < 0 and cap > 0:
        return ProcSet()
    
    return acc

def traversal(avail, hier_levels, hier, init, cur, lv, bottom, policy):
    i = 0
    acc = []
    l = len(hier)

    while i < l and cur[lv] != 0:
        r = hier[i]
        logger.info(r)
        logger.info(init)
        logger.info(cur)
        children = [c for c in hier_levels[lv+1] if c.issubset(r)]
        if lv+1 == bottom:
            res = pick_n(avail, children, cur, lv+1, policy)
        else:
            res = traversal(avail, hier_levels, children, init, cur, lv+1, bottom, policy)
        if res:
            acc.insert(0, res)
            if cur[lv+1] == 0:
                cur[lv+1] = init[lv+1]
                cur[lv] = 0 if cur[lv] < 0 else cur[lv]-1
            elif cur[lv+1] < 0:
                if lv+1 == bottom:
                    if cur[lv] > 0:
                        cur[lv] -= 1
                else:
                    if (policy == 'compact' and len(acc) == len(children)) or len(acc) == 2*len(children):
                        cur[lv] = 0 if cur[lv] < 0 else cur[lv]-1
        i += 1
    
    if lv == 0 and all(v != 0 for v in cur):
        return []
    return acc

def my_find(itvs_slots, hy_res_rqts, hy, beginning_slotset, *args):

    result = ProcSet()
    
    hy_keys, hy_levels = [list(t) for t in zip(*hy.items())]
    hy_nbs = [-1 for lv in hy_levels]

    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_nbs[hy_keys.index(l_name)] = n

    itvs_cts_slots = constraints & itvs_slots
    hy_levels[0].sort(key=lambda n: len(n & itvs_cts_slots), reverse = True)
    
    rset = traversal(itvs_cts_slots, hy_levels, hy_levels[0], hy_nbs, hy_nbs[:], 0, len(hy_levels)-2, args[0])

    rset = [i for sublist in rset for i in sublist]
    
    res = ProcSet()
    
    for s in rset:
        res = res | s 

    logger.info(res)
    if res:
        result = result | res
    else:
        return ProcSet()

    return result
