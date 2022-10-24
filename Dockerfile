FROM ghcr.io/binkhq/python:3.10-poetry as build
WORKDIR /src
ADD . .
RUN poetry build

FROM ghcr.io/binkhq/python:3.10

WORKDIR /app
COPY --from=build /src/dist/*.whl .
RUN pip install *.whl && rm *.whl

ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "/usr/local/bin/wasabi_reporter" ]
