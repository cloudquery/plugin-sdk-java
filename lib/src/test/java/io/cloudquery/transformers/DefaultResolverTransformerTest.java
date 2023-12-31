package io.cloudquery.transformers;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cloudquery.scalar.ValidationException;
import io.cloudquery.schema.Column;
import io.cloudquery.schema.Resource;
import io.cloudquery.transformers.ResolverTransformer.DefaultResolverTransformer;
import lombok.Builder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultResolverTransformerTest {

  @Builder
  public static class ResourceItem {
    public String myCustomID;
  }

  private DefaultResolverTransformer transformer;

  @Mock private Resource resource;

  @BeforeEach
  void setUp() {
    transformer = new DefaultResolverTransformer();

    when(resource.getItem()).thenReturn(ResourceItem.builder().myCustomID("1234").build());
  }

  @Test
  public void shouldTransformCustomFieldNamesFromResource()
      throws TransformerException, ValidationException {
    Column targetColumn = Column.builder().name("id").build();

    transformer.transform(null, "myCustomID").resolve(null, resource, targetColumn);

    verify(resource).set(eq("id"), eq("1234"));
  }

  @Test
  public void shouldThrowExceptionIfResourceFieldNameNotFound() throws TransformerException {
    Column targetColumn = Column.builder().name("id").build();

    assertThrows(
        TransformerException.class,
        () -> transformer.transform(null, "badFieldName").resolve(null, resource, targetColumn));
  }
}
