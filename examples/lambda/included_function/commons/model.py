from torch.jit import RecursiveScriptModule
import torch

class OffSamplePredictModel:
    def __init__(self, model_path):
        self.jit_model = torch.jit.load(model_path,torch.device('cpu'))

    def postprocessing(self, out):
        labels = []
        probabilities = []
        out = torch.softmax(out, dim=1)
        pred_probs = out.numpy()
        preds = pred_probs.argmax(axis=1)  # _, preds = torch.max(out.data, 1)
        for i in range(len(pred_probs)):
            probabilities.append(pred_probs[i][0])
            if (preds[i] == 0):
                labels.append('off')
            else:
                labels.append('on')

        return probabilities, labels

    def predict(self, tensors):
        tensors = torch.stack(tensors)
        # resnet.eval()
        with torch.no_grad():
            out = self.jit_model.forward(tensors)

        return self.postprocessing(out)

class OffSampleTorchscriptFork:
    def __init__(self, ens: RecursiveScriptModule):
        self.ens = ens

    def postprocessing(self, out):
        labels = []
        probabilities = []
        out = torch.softmax(out, dim=1)
        pred_probs = out.numpy()
        preds = pred_probs.argmax(axis=1)  # _, preds = torch.max(out.data, 1)
        for i in range(len(pred_probs)):
            probabilities.append(pred_probs[i][0])
            if (preds[i] == 0):
                labels.append('off')
            else:
                labels.append('on')
        pred_list = [{'prob': float(prob), 'label': label} for prob, label in zip(probabilities, labels)]
        return pred_list


    def predict(self, tensors):
        tensors = torch.stack(tensors)
        with torch.no_grad():
            out = self.ens(tensors)
        pred_list = self.postprocessing(out)
        return pred_list


