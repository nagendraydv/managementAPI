from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
import falcon
from falcon_apispec import FalconPlugin
from marshmallow import Schema, fields


# Optional marshmallow support
class CategorySchema(Schema):
    id = fields.Int()
    name = fields.Str(required=True)

class PetSchema(Schema):
    category = fields.Nested(CategorySchema, many=True)
    name = fields.Str()


# Create Falcon web app
app = falcon.API()

class RandomPetResource:
    def on_get(self, req, resp):
        """A cute furry animal endpoint.
        ---
        description: Get a random pet
        responses:
            200:
                description: A pet to be returned
                schema: PetSchema
        """
        pet = {'Name':'LAb','Age':11}  # returns JSON
        resp.media = pet

# create instance of resource
random_pet_resource = RandomPetResource()
# pass into `add_route` for Falcon
app.add_route("/random", random_pet_resource)


# Create an APISpec
spec = APISpec(
    title='Swagger Petstore',
    version='1.0.0',
    openapi_version='2.0',
    plugins=[
        FalconPlugin(app),
        MarshmallowPlugin(),
    ],
)

# Register entities and paths
spec.components.schema('Category', schema=CategorySchema)
spec.components.schema('Pet', schema=PetSchema)
# pass created resource into `path` for APISpec
spec.path(resource=random_pet_resource)
print(type(spec.to_dict()))
import json
print(json.dumps(spec.to_dict()))
