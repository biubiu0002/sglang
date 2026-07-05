import torch

from sglang.srt.layers.logits_processor import LogitsProcessorOutput
from sglang.srt.model_executor.runner_backend.breakable_cuda_graph_backend import (
    BreakableCudaGraphBackend,
)


def test_breakable_backend_slices_logits_processor_output():
    output = LogitsProcessorOutput(
        next_token_logits=torch.arange(12).view(4, 3),
        hidden_states=torch.arange(20).view(4, 5),
        customized_info={"kept": True},
    )

    sliced = BreakableCudaGraphBackend._slice_output(None, output, 2)

    assert isinstance(sliced, LogitsProcessorOutput)
    assert sliced.next_token_logits.tolist() == [[0, 1, 2], [3, 4, 5]]
    assert sliced.hidden_states.tolist() == [
        [0, 1, 2, 3, 4],
        [5, 6, 7, 8, 9],
    ]
    assert sliced.customized_info == {"kept": True}


def test_breakable_backend_copies_logits_processor_output_to_buffer():
    output = LogitsProcessorOutput(
        next_token_logits=torch.arange(12).view(4, 3),
        hidden_states=torch.arange(20).view(4, 5),
    )
    output_buffer = LogitsProcessorOutput(
        next_token_logits=torch.full((4, 3), -1),
        hidden_states=torch.full((4, 5), -1),
    )

    BreakableCudaGraphBackend._copy_output_to_buffer(None, output, output_buffer, 2)

    assert output_buffer.next_token_logits.tolist() == [
        [0, 1, 2],
        [3, 4, 5],
        [-1, -1, -1],
        [-1, -1, -1],
    ]
    assert output_buffer.hidden_states.tolist() == [
        [0, 1, 2, 3, 4],
        [5, 6, 7, 8, 9],
        [-1, -1, -1, -1, -1],
        [-1, -1, -1, -1, -1],
    ]
