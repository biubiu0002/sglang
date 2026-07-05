import unittest
import importlib.util
import sys
import types
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from unittest.mock import patch

import torch

from sglang.test.ci.ci_register import register_amd_ci, register_cuda_ci


register_cuda_ci(est_time=9, stage="base-b", runner_config="1-gpu-small")
register_amd_ci(est_time=9, suite="stage-b-test-1-gpu-small-amd")

_LORA_OPS_PATH = (
    Path(__file__).parents[4]
    / "python"
    / "sglang"
    / "srt"
    / "lora"
    / "torch_ops"
    / "lora_ops.py"
)
_spec = importlib.util.spec_from_file_location("lora_ops_under_test", _LORA_OPS_PATH)
_lora_ops = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_lora_ops)

_LORA_MOE_RUNNERS_PATH = (
    Path(__file__).parents[4]
    / "python"
    / "sglang"
    / "srt"
    / "lora"
    / "lora_moe_runners.py"
)
_moe_spec = importlib.util.spec_from_file_location(
    "lora_moe_runners_under_test", _LORA_MOE_RUNNERS_PATH
)
_lora_moe_runners = importlib.util.module_from_spec(_moe_spec)
assert _moe_spec.loader is not None
sys.modules[_moe_spec.name] = _lora_moe_runners
_runner_stub = types.ModuleType("sglang.srt.model_executor.runner")
_runner_stub.get_is_capture_mode = lambda: False
_utils_stub = types.ModuleType("sglang.srt.utils")
_utils_stub.is_cuda = lambda: False
_utils_stub.is_hip = lambda: False
_utils_stub.is_xpu = lambda: False
_utils_stub.next_power_of_2 = lambda x: 1 << (x - 1).bit_length()
with patch.dict(
    sys.modules,
    {
        "sglang.srt.model_executor.runner": _runner_stub,
        "sglang.srt.utils": _utils_stub,
    },
):
    _moe_spec.loader.exec_module(_lora_moe_runners)


class TestLoRANoopRanks(unittest.TestCase):
    def test_rank_zero_skips_zero_weight_a_gemm_with_inf_input(self):
        inputs = torch.tensor([[float("inf"), 1.0]], dtype=torch.float32)
        weights = torch.zeros((1, 2, 2), dtype=torch.float32)

        output = _lora_ops.sgemm_lora_a_fwd(
            inputs=inputs,
            weights=weights,
            weight_indices=torch.tensor([0], dtype=torch.int32),
            seg_len_tensor=torch.tensor([1], dtype=torch.int32),
            lora_ranks=torch.tensor([0], dtype=torch.int32),
            scaling_tensor=torch.tensor([1.0], dtype=torch.float32),
        )

        self.assertTrue(torch.isfinite(output).all())
        torch.testing.assert_close(output, torch.zeros_like(output))

    def test_rank_zero_keeps_base_output_unchanged_with_inf_input(self):
        inputs = torch.tensor([[float("inf"), 1.0]], dtype=torch.float32)
        weights = torch.zeros((1, 2, 2), dtype=torch.float32)
        base_output = torch.tensor([[3.0, 4.0]], dtype=torch.float32)

        output = _lora_ops.sgemm_lora_b_fwd(
            inputs=inputs,
            weights=weights,
            weight_indices=torch.tensor([0], dtype=torch.int32),
            seg_len_tensor=torch.tensor([1], dtype=torch.int32),
            lora_ranks=torch.tensor([0], dtype=torch.int32),
            slice_offsets=torch.tensor([0, 2], dtype=torch.int32),
            base_output=base_output.clone(),
        )

        self.assertTrue(torch.isfinite(output).all())
        torch.testing.assert_close(output, base_output)

    def test_backend_rank_override_restores_original_batch_info(self):
        @dataclass
        class FakeBatchInfo:
            lora_ranks: torch.Tensor
            lora_ranks_cpu: torch.Tensor
            marker: int

        class FakeBackend:
            @contextmanager
            def use_lora_ranks(self, lora_ranks, lora_ranks_cpu=None):
                old_batch_info = self.batch_info
                updates = {"lora_ranks": lora_ranks}
                if lora_ranks_cpu is not None:
                    updates["lora_ranks_cpu"] = lora_ranks_cpu
                self.batch_info = replace(self.batch_info, **updates)
                try:
                    yield
                finally:
                    self.batch_info = old_batch_info

        backend = FakeBackend()
        original_ranks = torch.tensor([4], dtype=torch.int32)
        original_ranks_cpu = torch.tensor([4], dtype=torch.int32)
        backend.batch_info = FakeBatchInfo(original_ranks, original_ranks_cpu, marker=7)

        module_ranks = torch.tensor([0], dtype=torch.int32)
        module_ranks_cpu = torch.tensor([0], dtype=torch.int32)
        with backend.use_lora_ranks(module_ranks, module_ranks_cpu):
            self.assertIs(backend.batch_info.lora_ranks, module_ranks)
            self.assertIs(backend.batch_info.lora_ranks_cpu, module_ranks_cpu)
            self.assertEqual(backend.batch_info.marker, 7)

        self.assertIs(backend.batch_info.lora_ranks, original_ranks)
        self.assertIs(backend.batch_info.lora_ranks_cpu, original_ranks_cpu)

    def test_module_active_check_uses_only_real_lora_slots(self):
        class FakeLayer:
            def has_active_lora_for_current_batch(self, batch_info=None):
                if getattr(batch_info, "use_cuda_graph", False):
                    return True
                active_weight_indices = getattr(batch_info, "active_weight_indices", None)
                if active_weight_indices is None:
                    return True
                return any(
                    self.lora_ranks_cpu[idx].item() > 0
                    for idx in active_weight_indices
                )

        @dataclass
        class FakeBatchInfo:
            active_weight_indices: tuple[int, ...]
            use_cuda_graph: bool = False

        layer = FakeLayer()
        layer.lora_ranks_cpu = torch.tensor([0, 4], dtype=torch.int32)

        self.assertFalse(
            layer.has_active_lora_for_current_batch(FakeBatchInfo(tuple()))
        )
        self.assertFalse(
            layer.has_active_lora_for_current_batch(FakeBatchInfo((0,)))
        )
        self.assertTrue(
            layer.has_active_lora_for_current_batch(FakeBatchInfo((1,)))
        )
        self.assertTrue(
            layer.has_active_lora_for_current_batch(
                FakeBatchInfo(tuple(), use_cuda_graph=True)
            )
        )

    def test_moe_lora_naive_alignment_filters_invalid_expert_ids(self):
        sorted_token_ids, expert_ids, num_tokens_post_padded = (
            _lora_moe_runners._naive_moe_lora_align_block_size(
                topk_ids=torch.tensor([[0, 1, 2, 3, -1]], dtype=torch.int32),
                seg_indptr=torch.tensor([0, 1], dtype=torch.int32),
                req_to_lora=torch.tensor([0], dtype=torch.int32),
                num_experts=2,
                block_size_m=2,
                max_loras=1,
                max_num_tokens_padded=8,
                max_num_m_blocks=4,
                adapter_enabled=torch.tensor([1], dtype=torch.int32),
                expert_map=None,
                device=torch.device("cpu"),
            )
        )

        self.assertEqual(num_tokens_post_padded.tolist(), [4])
        self.assertEqual(sorted_token_ids[:4].tolist(), [0, 5, 1, 5])
        self.assertEqual(expert_ids.tolist(), [0, 1, -1, -1])

    def test_moe_lora_naive_alignment_filters_missing_adapter_experts(self):
        sorted_token_ids, expert_ids, num_tokens_post_padded = (
            _lora_moe_runners._naive_moe_lora_align_block_size(
                topk_ids=torch.tensor([[0, 1, 2, 3]], dtype=torch.int32),
                seg_indptr=torch.tensor([0, 1], dtype=torch.int32),
                req_to_lora=torch.tensor([0], dtype=torch.int32),
                num_experts=4,
                block_size_m=2,
                max_loras=1,
                max_num_tokens_padded=8,
                max_num_m_blocks=4,
                adapter_enabled=torch.tensor([1], dtype=torch.int32),
                expert_map=torch.tensor([[-1, 1, -1, 3]], dtype=torch.int32),
                device=torch.device("cpu"),
            )
        )

        self.assertEqual(num_tokens_post_padded.tolist(), [4])
        self.assertEqual(sorted_token_ids[:4].tolist(), [1, 4, 3, 4])
        self.assertEqual(expert_ids.tolist(), [1, 3, -1, -1])


if __name__ == "__main__":
    unittest.main()
