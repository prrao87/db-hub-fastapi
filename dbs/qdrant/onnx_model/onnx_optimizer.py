"""
This script is a modified version of the method shown in this blog post:
https://www.philschmid.de/optimize-sentence-transformers

It uses the ONNX Runtime to dynamically optimize and quantize a sentence transformers model for better CPU performance.

Using the quantized version of `sentence-transformers/multi-qa-MiniLM-L6-cos-v1` allows us to:
  * Generate similar quality sentence embeddings as the original model, but with a roughly 1.8x speedup in vectorization time
  * Reduce the model size from 86 MB to around 63 MB, a roughly 26% reduction in file size
"""
from pathlib import Path

import torch
import torch.nn.functional as F
from optimum.onnxruntime import ORTModelForCustomTasks, ORTOptimizer, ORTQuantizer
from optimum.onnxruntime.configuration import AutoQuantizationConfig, OptimizationConfig
from sklearn.metrics.pairwise import cosine_similarity
from transformers import AutoModel, AutoTokenizer, Pipeline


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


class SentenceEmbeddingPipeline(Pipeline):
    def _sanitize_parameters(self, **kwargs):
        # We don't have any hyperameters to sanitize
        preprocess_kwargs = {}
        return preprocess_kwargs, {}, {}

    def preprocess(self, inputs):
        encoded_inputs = self.tokenizer(inputs, padding=True, truncation=True, return_tensors="pt")
        return encoded_inputs

    def _forward(self, model_inputs):
        outputs = self.model(**model_inputs)
        return {"outputs": outputs, "attention_mask": model_inputs["attention_mask"]}

    def postprocess(self, model_outputs):
        # Perform mean pooling
        sentence_embeddings = mean_pooling(
            model_outputs["outputs"], model_outputs["attention_mask"]
        )
        # Normalize embeddings
        sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
        return sentence_embeddings


def optimize_model(model_id: str, onnx_path: Path) -> None:
    """
    Optimize ONNX model for CPU performance
    """
    model = ORTModelForCustomTasks.from_pretrained(model_id, export=True)
    # Create ORTOptimizer and define optimization configuration
    optimizer = ORTOptimizer.from_pretrained(model)
    # Save models to local disk
    model.save_pretrained(onnx_path)
    tokenizer.save_pretrained(onnx_path)
    # Set optimization_level = 99 -> enable all optimizations
    optimization_config = OptimizationConfig(optimization_level=99)
    # Apply the optimization configuration to the model
    optimizer.optimize(
        optimization_config=optimization_config,
        save_dir=onnx_path,
    )


def quantize_optimized_model(onnx_path: Path) -> None:
    """
    Quantize an already optimized ONNX model for even better CPU performance
    """
    # Create ORTQuantizer and define quantization configuration
    quantizer = ORTQuantizer.from_pretrained(onnx_path, file_name="model_optimized.onnx")
    quantization_config = AutoQuantizationConfig.avx512_vnni(is_static=False, per_channel=True)
    # Apply the quantization configuration to the model
    quantizer.quantize(
        quantization_config=quantization_config,
        save_dir=onnx_path,
    )


def generate_similarities(source_sentence: str, sentences: list[str], pipeline: Pipeline) -> None:
    source_sentence_embedding = pipeline(source_sentence).tolist()[0]

    for sentence in sentences:
        sentence_embedding = pipeline(sentence).tolist()[0]
        similarity = cosine_similarity([source_sentence_embedding], [sentence_embedding])[0]
        print(f"Similarity between '{source_sentence}' and '{sentence}': {similarity}")


def main() -> None:
    """
    Generate optimized and quantized ONNX models from a vanilla sentence transformer model
    """
    # Init vanilla sentence transformer pipeline
    print("---\nLoading vanilla sentence transformer model\n---")
    vanilla_pipeline = SentenceEmbeddingPipeline(model=vanilla_model, tokenizer=tokenizer)
    # Print out pairwise similarities
    generate_similarities(source_sentence, sentences, vanilla_pipeline)

    # Save model to ONNX
    Path("onnx").mkdir(exist_ok=True)
    onnx_path = Path("onnx")

    # First, dynamically optimize an existing sentence transformer model
    optimize_model(model_id, onnx_path)
    # Next, dynamically quantize the optimized model
    quantize_optimized_model(onnx_path)

    # Init quantized ONNX pipeline
    print("---\nLoading quantized ONNX model\n---")
    model_filename = "model_optimized_quantized.onnx"
    quantized_model = ORTModelForCustomTasks.from_pretrained(onnx_path, file_name=model_filename)
    quantized_pipeline = SentenceEmbeddingPipeline(model=quantized_model, tokenizer=tokenizer)
    # Print out pairwise similarities
    generate_similarities(source_sentence, sentences, quantized_pipeline)


if __name__ == "__main__":
    # Example sentences we want sentence embeddings for
    source_sentence = "I'm very happy"
    sentences = ["I am so glad", "I'm so sad", "My dog is missing", "The universe is so vast!"]

    model_id = "sentence-transformers/multi-qa-MiniLM-L6-cos-v1"
    # Load AutoModel from huggingface model repository
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    vanilla_model = AutoModel.from_pretrained(model_id)

    main()
