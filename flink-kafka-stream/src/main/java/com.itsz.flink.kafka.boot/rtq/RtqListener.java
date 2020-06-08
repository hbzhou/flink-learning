/**
 * Copyright (c) 2016 Haitong International Securities Group Limited.
 * All rights reserved.
 * <p>
 * This file contains the valuable properties of Haitong International
 * Securities Group Limited. , embodying substantial creative efforts and confidential
 * information, ideas and expressions. No part of this file may be
 * reproduced or distributed in any form or by any means, or stored
 * in a data base or a retrieval system, without the prior written
 * permission of Haitong International Securities Group Limited.
 */
package com.itsz.flink.kafka.boot.rtq;

import com.taifook.rtq.client.tcp.RTQResponse;

/**
 *
 */
public interface RtqListener {

    public void receiveRtqData(RTQResponse response);
}
