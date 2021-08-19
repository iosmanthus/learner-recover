module.exports = {
    isMatch: function (a, b) {
        let kvsA = Object.entries(a);
        let kvsB = Object.entries(b);

        for (const [k, v] of kvsA) {
            if (b[k] !== v) {
                return false;
            }
        }
        return true;
    }
}