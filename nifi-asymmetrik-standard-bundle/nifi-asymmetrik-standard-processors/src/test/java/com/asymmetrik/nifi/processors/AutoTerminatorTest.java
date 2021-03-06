/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class AutoTerminatorTest {

    @Test
    public void testBulkSingle() {
        final TestRunner runner = TestRunners.newTestRunner(new AutoTerminator());
        runner.setProperty(AutoTerminator.BULK, "1");
        runner.enqueue("sample1".getBytes());
        runner.enqueue("sample2".getBytes());
        runner.enqueue("sample3".getBytes());
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testBulk() {
        final TestRunner runner = TestRunners.newTestRunner(new AutoTerminator());
        runner.setProperty(AutoTerminator.BULK, "10");
        runner.enqueue("sample1".getBytes());
        runner.enqueue("sample2".getBytes());
        runner.enqueue("sample3".getBytes());
        runner.run();
        runner.assertQueueEmpty();
    }
}
