// // Copyright 2017-2019, Schlumberger
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //      http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// package org.opengroup.osdu.storage.model;

// import static junit.framework.TestCase.assertEquals;
// import static org.junit.Assert.assertFalse;
// import static org.junit.Assert.assertTrue;

// import org.junit.Test;

// import com.google.common.base.Strings;
// import org.opengroup.osdu.core.common.model.storage.Record;

// public class RecordTest {

//     @Test
//     public void createNewRecordId() {

//         final String TENANT = "myTenant";

//         Record record = new Record();
//         record.createNewRecordId(TENANT);

//         String idTokens[] = record.getId().split(":");

//         assertEquals(3, idTokens.length);
//         assertEquals(TENANT, idTokens[0]);
//         assertEquals("doc", idTokens[1]);
//         assertFalse(Strings.isNullOrEmpty(idTokens[2]));
//     }

//     @Test
//     public void should_validateWhetherRecordMatchesTenantName() {
//         assertTrue(Record.isRecordIdValid("tenant1:well:123", "TENANT1"));
//         assertTrue(Record.isRecordIdValid("TENaNT1:doc:123", "TENaNT1"));
//         assertFalse(Record.isRecordIdValid("tenant1:well:123", "abc"));
//     }
// }