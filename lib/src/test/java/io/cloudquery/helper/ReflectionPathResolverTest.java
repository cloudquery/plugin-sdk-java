package io.cloudquery.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import lombok.Builder;
import org.junit.jupiter.api.Test;

class ReflectionPathResolverTest {
  @Builder
  private static class TestClass {
    private String name;

    @Builder.Default private List<Integer> numbers = List.of(1, 2, 3);

    private TestClass singleChild;

    private List<TestClass> multipleChildren;
  }

  private static final TestClass TEST_DATA =
      TestClass.builder()
          .name("root")
          .singleChild(TestClass.builder().name("single-child1").build())
          .multipleChildren(
              List.of(
                  TestClass.builder().name("multi-child1").build(),
                  TestClass.builder().name("multi-child2").build()))
          .build();

  @Test
  public void shouldResolveSimpleFields() throws ReflectionPathResolver.PathResolverException {
    assertEquals("root", ReflectionPathResolver.resolve(TEST_DATA, "name"));
    assertEquals(List.of(1, 2, 3), ReflectionPathResolver.resolve(TEST_DATA, "numbers"));
  }

  @Test
  public void shouldResolveNestedField() throws ReflectionPathResolver.PathResolverException {
    assertEquals("single-child1", ReflectionPathResolver.resolve(TEST_DATA, "singleChild.name"));
  }

  @Test
  public void shouldThrowAnErrorIfWeEncounterACollection() {
    assertThrows(
        ReflectionPathResolver.PathResolverException.class,
        () -> ReflectionPathResolver.resolve(TEST_DATA, "multiplChildren.name"));
  }
}
