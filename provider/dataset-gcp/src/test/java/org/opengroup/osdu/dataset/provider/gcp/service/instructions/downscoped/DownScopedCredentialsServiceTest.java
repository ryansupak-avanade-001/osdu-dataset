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
	DownScopedOptions downScopedOptions;

	@Mock
	GoogleCredentials googleCredentials;

	@InjectMocks
	DownScopedCredentialsService downScopedCredentialsService;

	@Test
	public void givenService_whenRequestDownscopedCredentials_thenCreatedWithProperArgs() {

		DownScopedCredentials dsc = downScopedCredentialsService
			.getDownScopedCredentials(googleCredentials, downScopedOptions);
		verify(googleCredentials).createScoped(anyCollectionOf(String.class));
		assertEquals(Whitebox.getInternalState(dsc, "downScopedOptions"), downScopedOptions);
	}
}