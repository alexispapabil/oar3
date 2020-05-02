# coding: utf-8
""" Functions to handle queues"""
# QUEUES MANAGEMENT
# TODO is_waiting_job_specific_queue_present($$);
from oar.lib import (db, Queue, get_logger)
from itertools import groupby


logger = get_logger('oar.lib.queue')


def get_all_queue_by_priority():
    return db.query(Queue).order_by(Queue.priority.desc()).all()


def get_queues_by_priority():
    queues_ordered = db.query(Queue).order_by(Queue.priority.desc()).all()
    res = []

    for key, queues_group in groupby(queues_ordered, lambda q: q.priority):
        res_grp = []
        for queue in queues_group:
            res_grp.append(queue)
        res.append(res_grp)
    return res


def stop_queue(name):
    """ Stop a queue"""
    db.query(Queue).filter(Queue.name == name).update({Queue.state: 'notActive'},
                                                      synchronize_session=False)
    db.commit()


def start_queue(name):
    """ Start a queue"""
    db.query(Queue).filter(Queue.name == name).update({Queue.state: 'Active'},
                                                      synchronize_session=False)
    db.commit()


def stop_all_queues():
    """ Stop all queues"""
    db.query(Queue).update({Queue.state: 'notActive'},
                           synchronize_session=False)
    db.commit()


def start_all_queues():
    """ Start all queues"""
    db.query(Queue).update({Queue.state: 'Active'}, synchronize_session=False)
    db.commit()
    

def create_queue(name, priority, policy):
    Queue.create(name=name, priority=priority, scheduler_policy=policy, state='Active')
    db.commit()


def change_queue(name, priority, policy):
    db.query(Queue).filter(Queue.name == name).update({Queue.priority: priority,
                                                       Queue.scheduler_policy: policy},
                                                      synchronize_session=False)
    db.commit()


def remove_queue(name):
    db.query(Queue).filter(Queue.name == name).delete(synchronize_session=False)
    db.commit()
