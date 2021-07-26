export function getNowUTC() {
    return new Date();
}

export function parseDate(value: string): Date {
    const dateTimeRegex = new RegExp(
        /^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$/
    );
    const dateRegex = new RegExp(/^[\d]{4}-\d{2}-\d{2}$/);
    if (value.match(dateTimeRegex)) {
        return new Date(value);
    }

    if (value.match(dateRegex)) {
        const year = parseInt(value.substr(0, 4));
        const month = parseInt(value.substr(5, 2));
        const day = parseInt(value.substr(8, 2));
        return new Date(year, month - 1, day, 0, 0, 0, 0);
    }

    throw Error("Not a date: " + value);
}
