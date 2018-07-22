from __future__ import division, print_function, absolute_import
import tensorflow as tf
import pprint
import pandas as pd
import time
import numpy as np


#filename_queue = tf.train.string_input_producer(['datasets/normal/FB_output.csv'])

data = pd.read_csv('datasets/normal/FB_output.csv', header=0)

data = data.sample(frac=1)

features, labels = data, data.pop('Profit')
curr_batch = 0

num_gpus = 4
num_steps = 200
learning_rate = 0.001
batch_size = 1024
display_step = 10

num_input = 13
dropout = 0.75

def next_batch(size):
    start = curr_batch * size
    end = (curr_batch + 1) * size
    curr_batch += 1
    return features[start:end], labels[start:end] 

def neural_net(x, dropout, reuse, is_training):
    with tf.variable_scope('NeuralNet', reuse=reuse):
        x = tf.layers.dense(x, 20, activation=tf.nn.relu)
        x = tf.layers.dropout(x, rate=dropout, training=is_training)

        x = tf.layers.dense(x, 20, activation=tf.nn.relu)
        x = tf.layers.dropout(x, rate=dropout, training=is_training)

        out = tf.layers.dense(x, 1)
        out = tf.nn.softmax(out) if not is_training else out
    return out


def average_gradients(tower_grads):
    average_grads = []
    for grad_and_vars in zip(*tower_grads):
        grads = []
        for g, _ in grad_and_vars:
            expanded_g = tf.expand_dims(g, 0)
            grads.append(expanded_g)
        grad = tf.concat(grads, 0)
        grad = tf.reduce_mean(grad, 0)

        v = grad_and_vars[0][1]
        grad_and_var = (grad, v)
        average_grads.append(grad_and_var)
    return average_grads

PS_OPS = ['Variable', 'VariableV2', 'AutoReloadVariable']

def assign_to_device(device, ps_device='/cpu:0'):
    def _assign(op):
        node_def = op if isinstance(op, tf.NodeDef) else op.node_def
        if node_def.op in PS_OPS:
            return '/' + ps_device
        else:
            return device
    return _assign


with tf.device('/cpu:0'):
    tower_grads = []
    reuse_vars = False

    X = tf.placeholder(tf.float64, [None, num_input])
    Y = tf.placeholder(tf.float64, [None, 1])

    for i in range(num_gpus):
        with tf.device(assign_to_device('/gpu:{}'.format(i), ps_device='/cpu:0')):
            _x = X[i * batch_size: (i + 1) * batch_size]
            _y = Y[i * batch_size: (i + 1) * batch_size]

            logits_train = neural_net(_x, dropout, reuse=reuse_vars, is_training=True)

            logits_test = neural_net(_x, dropout, reuse=True, is_training=False)
            
            loss_op = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=logits_train, labels=_y))
            optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
            grads = optimizer.compute_gradients(loss_op)

            if i == 0:
                correct_pred = tf.equal(tf.argmax(logits_test, 1), tf.argmax(_y, 1))
                accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float64))

            reuse_vars = True
            tower_grads.append(grads)
    tower_grads = average_gradients(tower_grads)
    train_op = optimizer.apply_gradients(tower_grads)

    init = tf.global_variables_initializer()

    with tf.Session() as sess:
        sess.run(init)

        for step in range(1, num_steps + 1):
            batch_x, batch_y = next_batch(batch_size * num_gpus)
            ts = time.time()
            sess.run(train_op, feed_dict={X: batch_x, Y: batch_y})
            te = time.time() - ts
            if step % display_step == 0 or step == 1:
                loss, acc = sess.run([loss_op, accuracy], feed_dict={X: batch_x, Y: batch_y})
                print("Step " + str(step) + ": Minibatch Loss= " + \
                        "{:.4f}".format(loss) + ", Training Accuracy= " + \
                        "{:.3f}".format(acc) + ", %i Examples/sec" % int(len(batch_x)/te))
            step += 1
        print("Optimization Finished!")


