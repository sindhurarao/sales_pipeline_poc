from transformation.transformation_factory import TransformationFactory

class Transformer:
    def apply(self, df, transformations):
        result = df
        for transformation in transformations:
            strategy = TransformationFactory.create(transformation)
            result = strategy.apply(result, transformation)
        return result