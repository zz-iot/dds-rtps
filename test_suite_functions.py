#################################################################
# Use and redistribution is source and binary forms is permitted
# subject to the OMG-DDS INTEROPERABILITY TESTING LICENSE found
# at the following URL:
#
# https://github.com/omg-dds/dds-rtps/blob/master/LICENSE.md
#
#################################################################

from rtps_test_utilities import ReturnCode, basic_check
import re
import pexpect
import queue
import time

# This constant is used to limit the maximum number of samples that tests that
# check the behavior needs to read. For example, checking that the data
# is received in order, or that OWNERSHIP works properly, etc...
MAX_SAMPLES_READ = 500

def test_size_receivers(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function is used by test cases that have two publishers and one
    subscriber. This tests check how many samples are received by the
    subscriber application with different sizes.

    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern.
    """
    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    sub_string = re.search(r'\w\s+\w+\s+[0-9]+ [0-9]+ \[([0-9]+)\]',
        child_sub.before + child_sub.after)
    if sub_string is None:
        return ReturnCode.DATA_NOT_RECEIVED

    first_sample_size = int(sub_string.group(1))

    max_samples_received = MAX_SAMPLES_READ
    samples_read = 0
    ignore_first_samples = True
    retcode = ReturnCode.RECEIVING_FROM_ONE

    while sub_string is not None and samples_read < max_samples_received:
        current_sample_size = int(sub_string.group(1))

        if current_sample_size != first_sample_size:
            if ignore_first_samples:
                # the first time we receive a different size, ignore it
                # For example, if we receive samples of size 20, then only
                # samples of size 30, we have to return RECEIVING_FROM_ONE.
                # If after receiving samples of size 30, we receive samples of
                # size 20 again, then we return RECEIVING_FROM_BOTH.
                ignore_first_samples = False
                first_sample_size = current_sample_size
            else:
                retcode = ReturnCode.RECEIVING_FROM_BOTH
                break

        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )

        if index == 1:
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

        samples_read += 1

        sub_string = re.search(r'\w\s+\w+\s+[0-9]+ [0-9]+ \[([0-9]+)\]',
            child_sub.before + child_sub.after)

    print(f'Samples read: {samples_read}')
    return retcode

def test_color_receivers(child_sub, samples_sent, last_sample_saved, timeout):

    """
    This function is used by test cases that have two publishers and one
    subscriber. This tests how many samples are received by the
    subscriber application with different colors.

    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern.
    """
    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    sub_string = re.search(r'\w\s+(\w+)\s+[0-9]+ [0-9]+ \[[0-9]+\]',
        child_sub.before + child_sub.after)
    first_sample_color = sub_string.group(1)

    max_samples_received = MAX_SAMPLES_READ
    samples_read = 0

    while sub_string is not None and samples_read < max_samples_received:
        current_sample_color = sub_string.group(1)

        # Check that all received samples have the same color
        if current_sample_color != first_sample_color:
            return ReturnCode.RECEIVING_FROM_BOTH

        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )

        if index == 1:
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

        samples_read += 1

        sub_string = re.search(r'\w\s+(\w+)\s+[0-9]+ [0-9]+ \[[0-9]+\]',
            child_sub.before + child_sub.after)

    print(f'Samples read: {samples_read}')
    return ReturnCode.RECEIVING_FROM_ONE

def test_size_less_than_20(child_sub, samples_sent, last_sample_saved, timeout):
    """
    Checks that all received samples have size between 1 and 20 (inclusive).
    Returns ReturnCode.OK if all samples are in range, otherwise ReturnCode.DATA_NOT_CORRECT.
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern.
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    max_samples_received = MAX_SAMPLES_READ / 2
    samples_read = 0
    return_code = ReturnCode.OK

    sub_string = re.search(r'[0-9]+ [0-9]+ \[([0-9]+)\]', child_sub.before + child_sub.after)

    while sub_string is not None and samples_read < max_samples_received:
        size = int(sub_string.group(1))
        if size < 1 or size > 20:
            return_code = ReturnCode.DATA_NOT_CORRECT
            break

        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 1 or index == 2:
            return_code = ReturnCode.DATA_NOT_RECEIVED
            break

        samples_read += 1
        sub_string = re.search(r'[0-9]+ [0-9]+ \[([0-9]+)\]', child_sub.before + child_sub.after)

    print(f'Samples read: {samples_read}')
    return return_code

def test_order_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests that the subscriber receives the samples in order
    (for several instances)
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern.
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.DATA_NOT_RECEIVED

    instance_color = []
    instance_seq_num = []
    first_iteration = []
    samples_read_per_instance = 0
    max_samples_received = MAX_SAMPLES_READ

    while samples_read_per_instance < max_samples_received:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
                instance_seq_num.append(int(sub_string.group(2)))
                first_iteration.append(True)

            if sub_string.group(1) in instance_color:
                index = instance_color.index(sub_string.group(1))
                if first_iteration[index]:
                    first_iteration[index] = False
                else:
                    # check that the next sequence number is the next value
                    current_size = int(sub_string.group(2))
                    if (current_size > instance_seq_num[index]):
                        instance_seq_num[index] = current_size
                    else:
                        produced_code = ReturnCode.DATA_NOT_CORRECT
                        break
            else:
                produced_code = ReturnCode.DATA_NOT_CORRECT
                break

        # Get the next sample the subscriber is receiving
        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                r'Reading with ordered access.*?\n', # index = 1
                pexpect.TIMEOUT, # index = 2
                pexpect.EOF # index = 3
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                samples_read_per_instance += 1
        elif index == 2:
            # no more data to process
            break
        elif index == 3:
            return ReturnCode.DATA_NOT_RECEIVED

    if max_samples_received == samples_read_per_instance:
        produced_code = ReturnCode.OK

    print(f'Samples read per instance: {samples_read_per_instance}, instances: {instance_color}')
    return produced_code

def test_reliability_no_losses_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests RELIABLE reliability, it checks whether the subscriber
    receives the samples in order and with no losses (for several instances)
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern.
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.DATA_NOT_RECEIVED

    instance_color = []
    instance_seq_num = []
    first_iteration = []
    samples_read_per_instance = 0
    max_samples_received = MAX_SAMPLES_READ

    while samples_read_per_instance < max_samples_received:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
                instance_seq_num.append(int(sub_string.group(2)))
                first_iteration.append(True)

            if sub_string.group(1) in instance_color:
                index = instance_color.index(sub_string.group(1))
                if first_iteration[index]:
                    first_iteration[index] = False
                else:
                    # check that the next sequence number is the next value
                    instance_seq_num[index] += 1
                    if instance_seq_num[index] != int(sub_string.group(2)):
                        produced_code = ReturnCode.DATA_NOT_CORRECT
                        break
            else:
                produced_code = ReturnCode.DATA_NOT_CORRECT
                break

        # Get the next sample the subscriber is receiving
        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                samples_read_per_instance += 1
        elif index == 1:
            # no more data to process
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

    if max_samples_received == samples_read_per_instance:
        produced_code = ReturnCode.OK

    print(f'Samples read per instance: {samples_read_per_instance}, instances: {instance_color}')
    return produced_code


def test_durability_volatile(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests the volatile durability, it checks that the sample the
    subscriber receives is not the first one. The publisher application sends
    samples increasing the value of the size, so if the first sample that the
    subscriber app doesn't have the size > 5, the test is correct.

    Note: size > 5 to avoid checking only the first sample, that may be an edge
          case where the DataReader hasn't matched with the DataWriter yet and
          the first samples are not received.

    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: not used
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    # Read the first sample, if it has the size > 5, it is using volatile
    # durability correctly
    sub_string = re.search(r'[0-9]+ [0-9]+ \[([0-9]+)\]',
        child_sub.before + child_sub.after)

    # Check if the element received is not the first 5 samples (aka size >= 5)
    # which should not be the case because the subscriber application waits some
    # seconds after the publisher. Checking 5 samples instead of just one to
    # make sure that there is not the case in which the DataReader hasn't
    # matched with the DataWriter yet and the first samples may not be received.
    # The group(1) contains the matching element for the parameter between
    # brackets in the regular expression. In this case is the size as a string.
    if int(sub_string.group(1)) >= 5:
        produced_code = ReturnCode.OK
    else:
        produced_code = ReturnCode.DATA_NOT_CORRECT

    return produced_code

def test_durability_transient_local(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests the TRANSIENT_LOCAL durability, it checks that the
    sample the subscriber receives is the first one. The publisher application
    sends samples increasing the value of the size, so if the first sample that
    the subscriber app does have the size == 1, the test is correct.

    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: not used
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    # Read the first sample, if it has the size == 1, it is using transient
    # local durability correctly
    sub_string = re.search(r'[0-9]+ [0-9]+ \[([0-9]+)\]',
        child_sub.before + child_sub.after)

    # Check if the element is the first one sent (aka size == 1), which should
    # be the case for TRANSIENT_LOCAL durability.
    # The group(1) contains the matching element for the parameter between
    # brackets in the regular expression. In this case is the size as a string.
    if int(sub_string.group(1)) == 1:
        produced_code = ReturnCode.OK
    else:
        produced_code = ReturnCode.DATA_NOT_CORRECT

    return produced_code


def test_deadline_missed(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests whether the subscriber application misses the requested
    deadline or not. This is needed in case the subscriber application receives
    some samples and then missed the requested deadline.

    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    # At this point, the subscriber app has already received one sample
    # Check deadline requested missed
    index = child_sub.expect([
            'on_requested_deadline_missed()', # index = 0
            pexpect.TIMEOUT, # index = 1
            pexpect.EOF # index = 2
        ],
        timeout)
    if index == 0:
        return ReturnCode.DEADLINE_MISSED
    elif index == 1:
        return ReturnCode.DATA_NOT_RECEIVED
    else:
        index = child_sub.expect([
            r'\[[0-9]+\]', # index = 0
            pexpect.TIMEOUT, # index = 1
            pexpect.EOF # index = 2
        ],
        timeout)
        if index == 0:
            return ReturnCode.OK
        else:
            return ReturnCode.DATA_NOT_RECEIVED

def test_reading_1_sample_every_10_samples_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests whether the subscriber application receives one sample
    out of 10 (for each instance). For example, first sample received with size
    5, the next one should have size [15,24], then [25,34], etc.
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.DATA_NOT_RECEIVED

    instance_color = []
    instance_seq_num = []
    first_iteration = []
    ignore_first_sample = []
    missed_samples = []
    max_samples_received = MAX_SAMPLES_READ / 10 # 50
    samples_read_per_instance = 0

    while samples_read_per_instance < max_samples_received:

        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
                instance_seq_num.append(int(sub_string.group(2)))
                first_iteration.append(True)
                ignore_first_sample.append(True)
                missed_samples.append(0)

            if sub_string.group(1) in instance_color:
                index = instance_color.index(sub_string.group(1))
                if first_iteration[index]:
                    first_iteration[index] = False
                else:
                    current_seq_num = int(sub_string.group(2))
                    if ignore_first_sample[index]:
                        ignore_first_sample[index] = False
                    else:
                        # check that the received sample reads only one sample in
                        # in the period of 10 samples. For example, if the previous
                        # sample received has size 5, the next one should be
                        # between [15-24], both included.
                        # As the write period does not take into account the
                        # execution overhead, the next valid sample may be
                        # between [14-24] if the filtering happens in the reader
                        # side.
                        # A gap larger than ~10 means one or more filtered samples
                        # were not received; count them against the 10% tolerance.
                        gap = current_seq_num - instance_seq_num[index]
                        if gap < 9:
                            # sample arrived too early
                            produced_code = ReturnCode.DATA_NOT_CORRECT
                            break
                        missed_in_gap = max(0, (gap - 10) // 10)
                        missed_samples[index] += missed_in_gap
                        if missed_samples[index] > max_samples_received * 0.1:
                            produced_code = ReturnCode.DATA_NOT_CORRECT
                            break
                    instance_seq_num[index] = current_seq_num
            else:
                produced_code = ReturnCode.DATA_NOT_CORRECT
                break

        # Get the next sample the subscriber is receiving
        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                # increase samples_read_per_instance only for the first instance
                samples_read_per_instance += 1
        elif index == 1:
            # no more data to process
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

    # Allow up to 10% of samples to be missing (not received)
    if samples_read_per_instance >= max_samples_received * 0.9:
        produced_code = ReturnCode.OK

    print(f'Samples read per instance: {samples_read_per_instance}, instances: {instance_color}')
    return produced_code

def test_unregistering_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests whether instances are correctly unregistered
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.OK

    instance_color = []
    unregistered_instance_color = []
    max_samples_received = MAX_SAMPLES_READ
    samples_read_per_instance = 0

    while samples_read_per_instance < max_samples_received:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
        else:
            # if no sample is received, it might be a UNREGISTER message
            sub_string = re.search(r'\w+\s+(\w+)\s+NOT_ALIVE_NO_WRITERS_INSTANCE_STATE',
                child_sub.before + child_sub.after)
            if sub_string is not None and sub_string.group(1) not in unregistered_instance_color:
                unregistered_instance_color.append(sub_string.group(1))
                if len(instance_color) == len(unregistered_instance_color):
                    break
        # Get the next sample the subscriber is receiving or unregister/dispose
        index = child_sub.expect(
            [
                r'\w+\s+\w+\s+.*?\n', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                samples_read_per_instance += 1
        elif index == 1:
            # no more data to process
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

    # compare that arrays contain the same elements and are not empty
    if len(instance_color) == 0:
        produced_code = ReturnCode.DATA_NOT_RECEIVED
    elif set(instance_color) == set(unregistered_instance_color):
        produced_code = ReturnCode.OK
    else:
        produced_code = ReturnCode.DATA_NOT_CORRECT

    print(f'Unregistered {len(unregistered_instance_color)} elements: {unregistered_instance_color}')

    return produced_code

def test_disposing_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests whether instances are correctly disposed
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.OK

    instance_color = []
    disposed_instance_color = []
    max_samples_received = MAX_SAMPLES_READ
    samples_read_per_instance = 0

    while samples_read_per_instance < max_samples_received:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
        else:
            # if no sample is received, it might be a DISPOSED message
            sub_string = re.search(r'\w+\s+(\w+)\s+NOT_ALIVE_DISPOSED_INSTANCE_STATE',
                child_sub.before + child_sub.after)
            if sub_string is not None and sub_string.group(1) not in disposed_instance_color:
                disposed_instance_color.append(sub_string.group(1))
                if len(instance_color) == len(disposed_instance_color):
                    break
        # Get the next sample the subscriber is receiving or unregister/dispose
        index = child_sub.expect(
            [
                r'\w+\s+\w+\s+.*?\n', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                samples_read_per_instance += 1
        elif index == 1:
            # no more data to process
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

    # compare that arrays contain the same elements and are not empty
    if len(instance_color) == 0:
        produced_code = ReturnCode.DATA_NOT_RECEIVED
    elif set(instance_color) == set(disposed_instance_color):
        produced_code = ReturnCode.OK
    else:
        produced_code = ReturnCode.DATA_NOT_CORRECT

    print(f'Disposed {len(disposed_instance_color)} elements: {disposed_instance_color}')

    return produced_code

def test_large_data(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests whether large data is correctly received
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.DATA_NOT_RECEIVED
    samples_read = 0

    while samples_read < MAX_SAMPLES_READ:
        # As the interoperability_report is just looking for the size [<size>],
        # this does not count the data after it, we need to read a full sample
        index = child_sub.expect(
            [
                r'\w+\s+\w+\s+[0-9]+\s+[0-9]+\s+\[[0-9]+\]\s+\{[0-9]+\}', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )

        if index == 0:
            # Read the sample received, if it prints the additional_bytes == 255,
            # it is sending large data correctly
            sub_string = re.search(r'\w+\s+\w+\s+[0-9]+\s+[0-9]+\s+\[[0-9]+\]\s+\{([0-9]+)\}',
                child_sub.before + child_sub.after)
            # Check if the last element of the additional_bytes field element is
            # received correctly
            if sub_string is not None:
                if int(sub_string.group(1)) != 255:
                    produced_code = ReturnCode.DATA_NOT_CORRECT
                    break
                samples_read += 1
            else:
                produced_code = ReturnCode.DATA_NOT_CORRECT
                break
        elif index == 1 or index == 2:
            produced_code = ReturnCode.DATA_NOT_RECEIVED
            break

    if samples_read == MAX_SAMPLES_READ:
        produced_code = ReturnCode.OK

    print(f'Samples read: {samples_read}')

    return produced_code

def test_lifespan_2_3_consecutive_samples_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests that lifespan works correctly. In the test situation,
    only 2 or 3 consecutive samples should be received each time the reader
    reads data.
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.DATA_NOT_RECEIVED

    # as the test is reading in a slower rate, reduce the number of samples read
    max_samples_lifespan = MAX_SAMPLES_READ / 10 # 50

    instance_color = []
    previous_seq_num = []
    first_iteration = []
    samples_read_per_instance = 0
    consecutive_samples = []
    ignore_first_sample = []
    lifespan_expiration_observed = False

    while samples_read_per_instance < max_samples_lifespan:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]',
            child_sub.before + child_sub.after)

        if sub_string is not None:
            # add a new instance to instance_color
            if sub_string.group(1) not in instance_color:
                instance_color.append(sub_string.group(1))
                previous_seq_num.append(int(sub_string.group(2)))
                first_iteration.append(True)
                consecutive_samples.append(1) # take into account the first sample
                ignore_first_sample.append(True)

            # if the instance exists
            if sub_string.group(1) in instance_color:
                index = instance_color.index(sub_string.group(1))
                # we should receive only 2 or 3 consecutive samples with the
                # parameters defined by the test
                if first_iteration[index]:
                    # do nothing for the first sample received
                    first_iteration[index] = False
                else:
                    # if the sequence number is consecutive, increase the counter
                    if previous_seq_num[index] + 1 == int(sub_string.group(2)):
                        consecutive_samples[index] += 1
                        # if found consecutive samples, do not ignore the first sample
                        ignore_first_sample[index] = False
                    else:
                        # if the sequence number is not consecutive, check that we
                        # receive only 3 or 2 samples
                        if consecutive_samples[index] == 3 or consecutive_samples[index] == 2:
                            # reset value to 1, as this test consider that the first
                            # sample is consecutive with itself
                            consecutive_samples[index] = 1
                            produced_code = ReturnCode.OK
                            lifespan_expiration_observed = True
                        else:
                            if ignore_first_sample[index]:
                                # there may be a case in which we receive a sample
                                # and the next one is not consecutive, if that is the
                                # case, ignore it
                                ignore_first_sample[index] = False
                            else:
                                # if the amount of samples received is different than 3 or 2
                                # this is an error
                                produced_code = ReturnCode.DATA_NOT_CORRECT
                                break
                    previous_seq_num[index] = int(sub_string.group(2))

            else:
                produced_code = ReturnCode.DATA_NOT_CORRECT
                break

        # Get the next sample the subscriber is receiving
        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                pexpect.TIMEOUT, # index = 1
                pexpect.EOF # index = 2
            ],
            timeout
        )
        if index == 0:
            if sub_string is not None and sub_string.group(1) == instance_color[0]:
                # increase samples_read_per_instance only for the first instance
                samples_read_per_instance += 1
        elif index == 1:
            # no more data to process
            break
        elif index == 2:
            return ReturnCode.DATA_NOT_RECEIVED

    if max_samples_lifespan == samples_read_per_instance:
        if lifespan_expiration_observed:
            produced_code = ReturnCode.OK
        else:
            # every sample arrived with no gaps at all: the peer never
            # actually expired anything, so this must not be reported as a
            # pass even though enough samples were read
            produced_code = ReturnCode.DATA_NOT_CORRECT

    print(f'Samples read per instance: {samples_read_per_instance}, instances: {instance_color}')
    return produced_code

def ordered_access_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    This function tests that ordered access works correctly. This counts the
    samples received in order and detects whether they are from the same instance
    as the previously received sample or not.
    If the number of consecutive samples from the same instance is greater than
    the number of consecutive samples form different instances, this means that
    the DW is using INSTANCE_PRESENTATION, if the case is the opposite, it is
    using TOPIC_PRESENTATION.
    child_sub: child program generated with pexpect
    samples_sent: not used
    last_sample_saved: not used
    timeout: time pexpect waits until it matches a pattern
    """

    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)

    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    produced_code = ReturnCode.OK

    instance_color = []
    samples_read_per_instance = 0
    previous_sample_color = None
    color_different_count = 0
    color_equal_count = 0
    samples_printed = False
    ordered_access_group_count = 0

    # There may be a timing issue when reading ordered access data. The
    # reader reads data consecutively, so the first time it reads the data it
    # may not get all expected samples, so the second time (before going to
    # sleep) it gets the rest of samples. This is not an error, so adding
    # some tolerance to the test
    behavior_mismatch_tolerance = int(0.01 * MAX_SAMPLES_READ)
    behavior_mismatch_count = 0

    while samples_read_per_instance < MAX_SAMPLES_READ:
        sub_string = re.search(r'\w+\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[[0-9]+\]',
            child_sub.before + child_sub.after)
        current_color = None

        # if a sample is read
        if sub_string is not None:
            # samples have been printed at least once
            samples_printed = True
            current_color = sub_string.group(1)

            # add new instance to instance_color
            if current_color not in instance_color:
                instance_color.append(current_color)

            # the instance exists
            if current_color in instance_color:
                index = instance_color.index(current_color)
                # check the previous color and increase the different or equal
                # counters
                if previous_sample_color is not None:
                    if current_color != previous_sample_color:
                        color_different_count += 1
                    else:
                        color_equal_count += 1
                previous_sample_color = current_color
        # different message than a sample
        else:
            sub_string = re.search(r'Reading with ordered access',
                child_sub.before + child_sub.after)
            # if 'Reading with ordered access' message, it means that the DataReader
            # is reading a new set of data (DataReader reads data slower that a
            # DataWriter writes it)
            if sub_string is not None:
                # if samples have been already received by the DataReader and the
                # counter addition (samples read) is greater than 5. It is 5 because
                # there are 4 instances and we need to make sure that we receive
                # at least 1 sample for every instance
                # Note: color_equal_count + color_different_count will be the samples read
                if samples_printed and color_equal_count + color_different_count > 5:
                    # if produced_code is not OK (this will happen in all iterations
                    # except for the first one). We check that the behavior is the same
                    # as in previous iterations.
                    if produced_code != ReturnCode.OK:
                        current_behavior = None
                        if color_equal_count > color_different_count:
                            current_behavior = ReturnCode.ORDERED_ACCESS_INSTANCE
                        elif color_equal_count < color_different_count:
                            current_behavior = ReturnCode.ORDERED_ACCESS_TOPIC

                        if produced_code != current_behavior:
                            if behavior_mismatch_count > behavior_mismatch_tolerance:
                                produced_code = ReturnCode.DATA_NOT_CORRECT
                                break
                            else:
                                behavior_mismatch_count += 1
                    # this only happens on the first iteration and then this sets
                    # the initial ReturnCode
                    else:
                        if color_equal_count > color_different_count:
                            produced_code = ReturnCode.ORDERED_ACCESS_INSTANCE
                        elif color_equal_count < color_different_count:
                            produced_code = ReturnCode.ORDERED_ACCESS_TOPIC
                # reset counters for the next set of samples read
                color_equal_count = 0
                color_different_count = 0

        # Get the next sample the subscriber is receiving or the next
        # 'Reading with ordered access message'
        index = child_sub.expect(
            [
                r'\[[0-9]+\]', # index = 0
                r'Reading with ordered access.*?\n', # index = 1
                pexpect.TIMEOUT, # index = 2
                pexpect.EOF # index = 3
            ],
            timeout
        )
        if index == 0:
            if current_color is not None and current_color == instance_color[0]:
                samples_read_per_instance += 1
        elif index == 1:
            ordered_access_group_count += 1
        elif index == 2:
            # no more data to process
            break
        elif index == 3:
            produced_code = ReturnCode.DATA_NOT_RECEIVED
            break

        # Exit condition in case there are no samples being printed
        if ordered_access_group_count > MAX_SAMPLES_READ:
            # If we have not read enough samples, we consider it a failure
            if samples_read_per_instance < MAX_SAMPLES_READ:
                produced_code = ReturnCode.DATA_NOT_RECEIVED
            break

    print(f'Samples read per instance: {samples_read_per_instance}, instances: {instance_color}')
    return produced_code

def coherent_sets_w_instances(child_sub, samples_sent, last_sample_saved, timeout):
    """
    Checks coherent-set delivery for the publisher configuration these tests use
    (--num-topics 3, --num-instances 4, --coherent-sample-count 3): one coherent
    set is 3 topics x 4 instances x 3 samples = 36 samples, and "size" increases
    by one on every write.

    A subscriber take()s on its own period, so a read can return part of a
    coherent set, one, or several.  Instead of assuming one read == one set,
    this collects every (topic, color, size) the subscriber prints and checks,
    over the whole run:
      * each instance's sizes are strictly increasing with no duplicates
      * when the publisher writes an instance's samples consecutively, the
        samples one read returns for that instance always come in whole
        multiples of --coherent-sample-count, i.e. an instance's coherent set is
        never split across reads.  A publisher that round-robins its instances
        interleaves the sets on the wire, so for it only the ordering check
        applies.
      * all topics/instances were seen and several coherent sets were received.
    """
    basic_check_retcode = basic_check(child_sub, samples_sent, last_sample_saved, timeout)
    if basic_check_retcode != ReturnCode.OK:
        return basic_check_retcode

    coherent_sample_count = 3                    # --coherent-sample-count
    num_topics, num_instances = 3, 4             # --num-topics / --num-instances
    sets_target = 25                            # stop once this many sets have arrived
    sets_min = 5                                # fewer than this => data did not flow

    samples = {}                               # (topic, color) -> [(read_index, size), ...]
    read_index = 0
    prev_key = None
    adjacent_same = adjacent_total = 0         # consecutive same-instance samples in the stream

    while True:
        sub_string = re.search(
            r'(\w+)\s+(\w+)\s+[0-9]+\s+[0-9]+\s+\[([0-9]+)\]\s*$',
            child_sub.before + child_sub.after)
        if sub_string is not None:
            key = (sub_string.group(1), sub_string.group(2))
            samples.setdefault(key, []).append((read_index, int(sub_string.group(3))))
            if prev_key is not None:
                adjacent_total += 1
                adjacent_same += (key == prev_key)
            prev_key = key
            if min((len(v) for v in samples.values()), default=0) >= sets_target * coherent_sample_count:
                break

        index = child_sub.expect(
            [
                r'\[[0-9]+\]',                   # index 0: a sample line
                r'Reading coherent sets.*?\n',   # index 1: subscriber starting a new read
                pexpect.TIMEOUT,                 # index 2
                pexpect.EOF,                     # index 3
            ],
            timeout)
        if index == 1:
            read_index += 1
            prev_key = None                     # a read boundary breaks the run
        elif index == 2 or index == 3:
            break

    if not samples:
        return ReturnCode.DATA_NOT_RECEIVED

    topics = {}
    for topic, color in samples:
        topics.setdefault(topic, set()).add(color)
    sets_received = min(len(v) for v in samples.values()) // coherent_sample_count
    print(f'Coherent sets: {sets_received} received, '
          f'{len(samples)} instances across {len(topics)} topics')
    if (len(topics) < num_topics
            or any(len(colors) < num_instances for colors in topics.values())
            or sets_received < sets_min):
        return ReturnCode.DATA_NOT_RECEIVED

    # instance-outer publisher: an instance\'s samples are written consecutively,
    # so most adjacent samples in the stream are the same instance.  round-robin
    # publisher: almost none are.
    instance_outer = adjacent_total > 0 and adjacent_same / adjacent_total > 0.3

    for (topic, color), lst in samples.items():
        sizes = [s for _, s in lst]
        for a, b in zip(sizes, sizes[1:]):
            if b <= a:
                print(f'{topic}/{color}: size {a} then {b} '
                      f'({"duplicate" if b == a else "out of order"})')
                return ReturnCode.DATA_NOT_CORRECT
        if not instance_outer:
            continue
        # number of this instance\'s samples returned by each read
        per_read = []
        for r, _ in lst:
            if per_read and per_read[-1][0] == r:
                per_read[-1][1] += 1
            else:
                per_read.append([r, 1])
        # the first and last read can be partial (join / shutdown); every read
        # in between must return a whole number of coherent sets for the instance
        for r, n in per_read[1:-1]:
            if n % coherent_sample_count != 0:
                print(f'{topic}/{color}: one read returned {n} samples '
                      f'(not a multiple of {coherent_sample_count}); '
                      f'coherent set split across reads')
                return ReturnCode.DATA_NOT_CORRECT

    return ReturnCode.OK
