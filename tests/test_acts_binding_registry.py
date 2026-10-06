"""How a binding is declared: card spellings, construction, and SUT environment.

Everything the runner needs to know about one binding lives with that
binding's dispatcher, so these tests check the declaration is complete for
every binding rather than for a hand-picked three — a binding added later is
covered without editing this file.
"""

from __future__ import annotations

import asyncio
import os

import pytest

import acts_runner
from test_suite.acts.dispatcher import (
    _CARD_SPELLINGS,
    GrpcDispatcher,
    JsonRpcDispatcher,
    RestDispatcher,
    binding_for_card,
    dispatcher_class,
)
from test_suite.acts.schema import TransportBinding


class TestCardSpellings:
    @pytest.mark.parametrize(
        ('spelling', 'binding'),
        [
            ('JSONRPC', TransportBinding.JSONRPC),
            ('GRPC', TransportBinding.GRPC),
            ('HTTP+JSON', TransportBinding.REST),
            ('HTTP_JSON', TransportBinding.REST),
            ('REST', TransportBinding.REST),
        ],
    )
    def test_each_spelling_resolves(self, spelling, binding):
        assert binding_for_card(spelling) is binding

    def test_spelling_is_case_insensitive(self):
        assert binding_for_card('grpc') is TransportBinding.GRPC

    def test_an_unknown_binding_resolves_to_nothing(self):
        assert binding_for_card('CARRIER-PIGEON') is None

    @pytest.mark.parametrize('binding', list(TransportBinding))
    def test_every_binding_is_reachable_from_a_card(self, binding):
        spellings = _CARD_SPELLINGS[binding]
        assert spellings
        assert all(binding_for_card(s) is binding for s in spellings)

    @pytest.mark.parametrize('binding', list(TransportBinding))
    def test_every_binding_has_a_dispatcher_that_speaks_it(self, binding):
        assert dispatcher_class(binding).binding is binding


class TestFromInterface:
    def test_grpc_drops_the_scheme_a_card_may_carry(self):
        dispatcher = GrpcDispatcher.from_interface(
            'http://127.0.0.1:5000/', agent_card_url='http://127.0.0.1:4000'
        )
        try:
            assert dispatcher.target == '127.0.0.1:5000'
        finally:
            asyncio.run(dispatcher.aclose())

    @pytest.mark.parametrize('cls', [JsonRpcDispatcher, RestDispatcher])
    def test_http_bindings_keep_the_mount_point_as_advertised(self, cls):
        dispatcher = cls.from_interface(
            'http://127.0.0.1:4000/jsonrpc/', agent_card_url='http://127.0.0.1:4000'
        )
        try:
            assert dispatcher.base_url == 'http://127.0.0.1:4000/jsonrpc/'
            assert dispatcher.agent_card_url == 'http://127.0.0.1:4000'
        finally:
            asyncio.run(dispatcher.aclose())


class TestSutEnvironment:
    @pytest.mark.parametrize(
        'binding',
        [TransportBinding.JSONRPC, TransportBinding.GRPC, TransportBinding.REST],
    )
    def test_the_standard_bindings_need_nothing(self, binding):
        async def entered():
            async with dispatcher_class(binding).sut_environment() as env:
                return env

        assert asyncio.run(entered()) == {}

    def test_exported_variables_are_restored(self, monkeypatch):
        monkeypatch.setenv('ITK_TEST_KEPT', 'before')
        monkeypatch.delenv('ITK_TEST_ADDED', raising=False)

        with acts_runner._exported({'ITK_TEST_KEPT': 'during', 'ITK_TEST_ADDED': 'x'}):
            assert os.environ['ITK_TEST_KEPT'] == 'during'
            assert os.environ['ITK_TEST_ADDED'] == 'x'

        assert os.environ['ITK_TEST_KEPT'] == 'before'
        assert 'ITK_TEST_ADDED' not in os.environ
