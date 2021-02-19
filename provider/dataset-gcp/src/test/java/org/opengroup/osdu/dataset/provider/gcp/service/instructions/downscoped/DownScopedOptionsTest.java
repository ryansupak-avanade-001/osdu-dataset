package org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class DownScopedOptionsTest {


	public static final String TEST_BUCKET_NAME = "openvds-test-data";
	public static final String TEST_FOLDER = "E5FC6BAA09EBC45A";
	public static final String TEST_AVAILABILITY_CONDITION_EXPRESSION =
		"resource.name.startsWith('projects/_/buckets/openvds-test-data/objects/<<folder>>/') " +
			"|| api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('<<folder>>')";

	@Test
	public void givenOptionsSettings_whenOptionsComposed_thenAllSetProperly() {

		String availabilityConditionExpression = TEST_AVAILABILITY_CONDITION_EXPRESSION
			.replaceAll("<<folder>>", TEST_FOLDER);

		AvailabilityCondition ap = new AvailabilityCondition("obj", availabilityConditionExpression);

		AccessBoundaryRule abr = new AccessBoundaryRule(
			"//storage.googleapis.com/projects/_/buckets/" + TEST_BUCKET_NAME,
			Collections.singletonList("inRole:roles/storage.objectViewer"),
			ap);

		DownScopedOptions downScopedOptions = new DownScopedOptions(Collections.singletonList(abr));

		assertTrue(downScopedOptions.getAccessBoundary().getAccessBoundaryRules().get(0).getAvailableResource()
			.contains(TEST_BUCKET_NAME));
	}

}