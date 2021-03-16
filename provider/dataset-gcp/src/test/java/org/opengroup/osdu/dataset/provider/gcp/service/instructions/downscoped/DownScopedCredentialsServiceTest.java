/*
 * Copyright 2021 Google LLC
 * Copyright 2021 EPAM Systems, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.verify;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.reflect.Whitebox;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class DownScopedCredentialsServiceTest {

	@Mock
	private DownScopedOptions downScopedOptions;

	@Mock
	private GoogleCredentials googleCredentials;

	@InjectMocks
	private DownScopedCredentialsService downScopedCredentialsService;

	@Test
	public void givenService_whenRequestDownscopedCredentials_thenCreatedWithProperArgs() {

		DownScopedCredentials dsc = downScopedCredentialsService
			.getDownScopedCredentials(googleCredentials, downScopedOptions);
		verify(googleCredentials).createScoped(anyCollectionOf(String.class));
		assertEquals(Whitebox.getInternalState(dsc, "downScopedOptions"), downScopedOptions);
	}
}