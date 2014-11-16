from slot import *
from scheduling import *

import random
import colorsys

import matplotlib.pyplot as plt
import matplotlib.patches as mpatch

NB_COLORS = 15
HSV_tuples = [(x*1.0/NB_COLORS, 0.5, 0.5) for x in range(NB_COLORS)]
RGB_tuples = map(lambda x: colorsys.hsv_to_rgb(*x), HSV_tuples)

def annotate(ax, rect, annot):            
    rx, ry = rect.get_xy()
    cx = rx + rect.get_width()/2.0
    cy = ry + rect.get_height()/2.0
    
    ax.annotate(annot, (cx, cy), color='black', 
                fontsize=12, ha='center', va='center')

def plot_slots_and_job(slots_set, jobs, nb_res, t_max):

    fig, ax = plt.subplots()

    for sid, slot in slots_set.slots.iteritems():
        col = "blue"
        if (sid % 2):
            col = "red"
        for i,itv in enumerate(slot.itvs):
            (y0, y1) = itv
            #print i, y0,y1, slot.b, slot.e
            #rect =  mpatch.Rectangle((2,2), 8, 2)
            rect = mpatch.Rectangle((slot.b, y0-0.4), slot.e-slot.b, 
                                    y1-y0+0.9, alpha = 0.1, color=col)
            if (i==0):
                annotate(ax, rect, 's'+str(sid))
            ax.add_artist(rect)

    for jid, job in jobs.iteritems():
        col = RGB_tuples[random.randint(0, NB_COLORS-1)]
        for i, itv in enumerate(job.res_set):
            (y0,y1) = itv
            rect = mpatch.Rectangle((job.start_time, y0-0.4), job.walltime, 
                                    y1-y0, alpha = 0.2, color=col)
            if (i==0):
                annotate(ax, rect, 'j'+str(jid))
            ax.add_artist(rect)
    
    ax.set_xlim((0, t_max))
    ax.set_ylim((0, nb_res))
#    ax.set_aspect('equal')
    ax.grid(True)
    mng = plt.get_current_fig_manager()
    try:
        mng.resize(*mng.window.maxsize())
        #mng.window.showMaximized()
    except:
        pass
    plt.show()



def slots_2_val_ref(slots):
    '''function used to generate reference value for unitest'''    
    sid = 1
    while True:
        slot = slots[sid]            
        print '(', slot.b, ',', slot.e, ',', slot.itvs, '),'
        sid = slot.next
        if (sid == 0):
            break

#j1 = Job(1,"", 10, 10, "", "", "", {}, [(10, 20), (25,30)], 1, [])
#j2 = Job(2,"", 5, 5, "", "", "", {}, [(1, 10), (15,20)], 1, [])
#slots_set = SlotSet(Slot(1, 0, 2, [(1, 32)], 1, 20))
#slots_set.slots[2]=Slot(2,1,0,[(10,15,),(21,30)],21,40)

j1 = Job(1,"", 5, 10, "", "", "", {}, [(10, 20)], 1, [])
j2 = Job(2,"", 30, 20, "", "", "", {}, [(5, 15),(20, 28)], 1, [])

res = [(1, 32)]
ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
all_ss = {0:ss}

hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

j3 = Job(3,"Waiting", 0, 0, "yop", "", "",{}, [], 0, 
         [ 
             (1, 60, 
              [  ( [("node", 2)], res)  ]
             )
         ]         
       ) 
assign_resources_mld_job_split_slots(ss, j3, hy)

#set_slots_with_prev_scheduled_jobs(all_ss, {1:j1, 2:j2}, [1,2], 10)

plot_slots_and_job(all_ss[0], {1:j3}, 40, 150)


#j4 = Job(1,"", 0 , 10, "", "", "", {}, [(10, 20)], 1, [])
#ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 0, 20))
#ss.split_slots(1,1,j4)
#ss.show_slots()



