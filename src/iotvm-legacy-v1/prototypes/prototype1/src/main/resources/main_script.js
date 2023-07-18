function performBinaryDecision (data) {
    var valueTemporary = JSON.parse(data.value);
    var valueObject = JSON.parse(valueTemporary);
    var value = valueObject.value;
    if (value > 27) {
        return true;
    } else {
        return false;
    }
}
